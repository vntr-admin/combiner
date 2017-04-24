package io.vntr.spj2;

import io.vntr.IRepManager;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.repartition.SpJ2Repartitioner;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

public class SpJ2Manager implements IRepManager {
    private int minNumReplicas;
    private float alpha;
    private float initialT;
    private float deltaT;
    private int k;

    private int nextPid = 1;
    private int nextUid = 1;

    private long migrationTally;
    private long logicalMigrationTally;
    private double logicalMigrationRatio;

    private Map<Integer, SpJ2Partition> pidToPartitionMap;
    private Map<Integer, Integer> uidToMasterPidMap = new HashMap<>();

    public SpJ2Manager(int minNumReplicas, float alpha, float initialT, float deltaT, int k, double logicalMigrationRatio) {
        this.minNumReplicas = minNumReplicas;
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        this.logicalMigrationRatio = logicalMigrationRatio;
        this.pidToPartitionMap = new HashMap<>();
    }

    @Override
    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public SpJ2Partition getPartitionById(Integer id) {
        return pidToPartitionMap.get(id);
    }

    @Override
    public RepUser getUserMasterById(Integer id) {
        Integer partitionId = uidToMasterPidMap.get(id);
        if (partitionId != null) {
            SpJ2Partition partition = getPartitionById(partitionId);
            if (partition != null) {
                return partition.getMasterById(id);
            }
        }
        return null;
    }

    @Override
    public int getNumUsers() {
        return uidToMasterPidMap.size();
    }

    @Override
    public Set<Integer> getPids() {
        return pidToPartitionMap.keySet();
    }

    @Override
    public Set<Integer> getUids() {
        return uidToMasterPidMap.keySet();
    }

    @Override
    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    @Override
    public void addUser(User user) {
        Integer masterPartitionId = getPartitionIdWithFewestMasters();

        int uid = user.getId();
        RepUser RepUser = new RepUser(uid);
        RepUser.setBasePid(masterPartitionId);

        addUser(RepUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(RepUser, id);
        }
    }

    @Override
    public void addUser(RepUser user, Integer masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        uidToMasterPidMap.put(user.getId(), masterPartitionId);

        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }
    }

    @Override
    public void removeUser(Integer userId) {
        RepUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getBasePid()).removeMaster(userId);
        for (Integer replicaPartitionId : user.getReplicaPids()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from uidToMasterPidMap
        uidToMasterPidMap.remove(userId);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            RepUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Integer friendReplicaPartitionId : friendMaster.getReplicaPids()) {
                SpJ2Partition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(userId);
            }
        }
    }

    @Override
    public Integer addPartition() {
        Integer newId = nextPid;
        addPartition(newId);
        return newId;
    }

    @Override
    public void addPartition(Integer pid) {
        pidToPartitionMap.put(pid, new SpJ2Partition(pid));
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    @Override
    public void removePartition(Integer id) {
        pidToPartitionMap.remove(id);
    }

    @Override
    public void addReplica(RepUser user, Integer destPid) {
        RepUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Integer pid : user.getReplicaPids()) {
            pidToPartitionMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    @Override
    public RepUser addReplicaNoUpdates(RepUser user, Integer destPid) {
        RepUser replica = user.dupe();
        pidToPartitionMap.get(destPid).addReplica(replica);
        return replica;
    }

    @Override
    public void removeReplica(RepUser user, Integer removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
            SpJ2Partition p = pidToPartitionMap.get(currentReplicaPartitionId);
            RepUser r = p.getReplicaById(user.getId());
            r.removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        pidToPartitionMap.get(removalPartitionId).removeReplica(user.getId());
    }

    @Override
    public void moveUser(RepUser user, Integer toPid, Set<Integer> replicateInDestinationPartition, Set<Integer> replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        Integer uid = user.getId();
        Integer fromPid = user.getBasePid();

        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for (Integer friendId : user.getFriendIDs()) {
            if (uidToMasterPidMap.get(friendId).equals(fromPid)) {
                addReplica(user, fromPid);
                break;
            }
        }

        for (Integer friendToReplicateId : replicateInDestinationPartition) {
            addReplica(getUserMasterById(friendToReplicateId), toPid);
        }

        //Step 3: remove unnecessary replicas
        //Possibilities:
        // (1) replica of user in destinationPartition
        // (2) replicas of user's friends in oldPartition with no other purpose
        // (3) [the replica of the new friend that prompted this move should already be accounted for in (2)]

        if (user.getReplicaPids().contains(toPid)) {
            if (user.getReplicaPids().size() <= minNumReplicas) {
                //add one in another partition that doesn't yet have one of this user
                addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user));
            }
            removeReplica(user, toPid);
        }

        //delete the replica of the appropriate friends in oldPartition
        for (Integer replicaIdToDelete : replicasToDeleteInSourcePartition) {
            removeReplica(getUserMasterById(replicaIdToDelete), fromPid);
        }

        increaseTally(1);
    }

    @Override
    public void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        RepUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        uidToMasterPidMap.put(uid, toPid);

        user.setBasePid(toPid);

        for (Integer rPid : user.getReplicaPids()) {
            pidToPartitionMap.get(rPid).getReplicaById(uid).setBasePid(toPid);
        }
    }

    @Override
    public void befriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPids()) {
            pidToPartitionMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPids()) {
            pidToPartitionMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    @Override
    public void unfriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPids()) {
            pidToPartitionMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPids()) {
            pidToPartitionMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    @Override
    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        SpJ2Partition partition = pidToPartitionMap.get(partitionId);
        RepUser user = partition.getReplicaById(userId);
        user.setBasePid(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        uidToMasterPidMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPids()) {
            RepUser replica = pidToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setBasePid(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }

        //Add replicas of friends in pid if they don't already exist
        for(int friendId : user.getFriendIDs()) {
            if(!partition.getIdsOfMasters().contains(friendId) && !partition.getIdsOfReplicas().contains(friendId)) {
                addReplica(getUserMasterById(friendId), partitionId);
            }
        }
    }

    @Override
    public Integer getPartitionIdWithFewestMasters() {
        int minMasters = Integer.MAX_VALUE;
        Integer minId = -1;

        for (Integer id : pidToPartitionMap.keySet()) {
            int numMasters = getPartitionById(id).getNumMasters();
            if (numMasters < minMasters) {
                minMasters = numMasters;
                minId = id;
            }
        }

        return minId;
    }

    @Override
    public Integer getRandomPartitionIdWhereThisUserIsNotPresent(RepUser user) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(pidToPartitionMap.keySet());
        potentialReplicaLocations.remove(user.getBasePid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        List<Integer> list = new LinkedList<>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    @Override
    public Set<Integer> getPartitionsToAddInitialReplicas(Integer masterPartitionId) {
        List<Integer> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<>(pidToPartitionMap.keySet());
        partitionIdsAtWhichReplicasCanBeAdded.remove(masterPartitionId);
        return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : pidToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToReplicasMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : pidToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    @Override
    public Integer getEdgeCut() {
        int count = 0;
        for (Integer uid : uidToMasterPidMap.keySet()) {
            RepUser user = getUserMasterById(uid);
            Integer pid = user.getBasePid();
            for (Integer friendId : user.getFriendIDs()) {
                if (pid < uidToMasterPidMap.get(friendId)) {
                    count++;
                }
            }
        }
        return count;
    }

    @Override
    public Integer getReplicationCount() {
        int count = 0;
        for(Integer pid : getPids()) {
            count += getPartitionById(pid).getNumReplicas();
        }
        return count;
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : uidToMasterPidMap.keySet()) {
            friendships.put(uid, getUserMasterById(uid).getFriendIDs());
        }
        return friendships;
    }

    @Override
    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * logicalMigrationTally);
    }

    @Override
    public void increaseTally(int amount) {
        migrationTally += amount;
    }

    @Override
    public void increaseTallyLogical(int amount) {
        logicalMigrationTally += amount;
    }

    @Override
    public String toString() {
        return "minNumReplicas:" + minNumReplicas + "|#U:" + getNumUsers() + "|#P:" + pidToPartitionMap.size();
    }

    @Override
    public void checkValidity() {

        //Check masters
        for(Integer uid : uidToMasterPidMap.keySet()) {
            Integer observedMasterPid = null;
            for(Integer pid : pidToPartitionMap.keySet()) {
                if(pidToPartitionMap.get(pid).getIdsOfMasters().contains(uid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot be in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must be in some partition");
            }
            if(!observedMasterPid.equals(uidToMasterPidMap.get(uid))) {
                throw new RuntimeException("Mismatch between uMap's location of user and partition's");
            }
            if(!observedMasterPid.equals(getUserMasterById(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }

            //TODO: should we check the logical partitions?
        }

        //check replicas
        for(Integer uid : uidToMasterPidMap.keySet()) {
            RepUser user = getUserMasterById(uid);
            Set<Integer> observedReplicaPids = new HashSet<>();
            for(Integer pid : pidToPartitionMap.keySet()) {
                if(pidToPartitionMap.get(pid).getIdsOfReplicas().contains(uid)) {
                    observedReplicaPids.add(pid);
                }
            }
            if(observedReplicaPids.size() < minNumReplicas) {
                throw new RuntimeException("Insufficient replicas");
            }
            if(!observedReplicaPids.equals(user.getReplicaPids())) {
                throw new RuntimeException("Mismatch between user's replica PIDs and system's");
            }
        }

        //check local semantics
        if(!checkLocalSemantics()) {
            throw new RuntimeException("local semantics issue!");
        }
    }

    private boolean checkLocalSemantics() {
        boolean valid = true;
        Map<Integer, Set<Integer>> friendships = getFriendships();
        Map<Integer, Set<Integer>> partitions  = getPartitionToUserMap();
        Map<Integer, Set<Integer>> replicas    = getPartitionToReplicasMap();
        for(int uid1 : friendships.keySet()) {
            for(int uid2 : friendships.get(uid1)) {
                Set<Integer> uid1Replicas = findKeysForUser(replicas, uid1);
                Set<Integer> uid2Replicas = findKeysForUser(replicas, uid2);
                int pid1 = findKeysForUser(partitions, uid1).iterator().next();
                int pid2 = findKeysForUser(partitions, uid2).iterator().next();
                if(pid1 != pid2) {
                    //If they aren't colocated, they have replicas in each other's partitions
                    valid &= uid1Replicas.contains(pid2);
                    valid &= uid2Replicas.contains(pid1);
                }
            }
        }
        return valid;
    }

    private static Set<Integer> findKeysForUser(Map<Integer, Set<Integer>> m, int uid) {
        Set<Integer> keys = new HashSet<>();
        for(int key : m.keySet()) {
            if(m.get(key).contains(uid)) {
                keys.add(key);
            }
        }
        return keys;
    }

    void repartition() {
        SpJ2Repartitioner.Results results = SpJ2Repartitioner.repartition(minNumReplicas, alpha, initialT, deltaT, k, getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap());
        increaseTallyLogical(results.getMoves());
        physicallyMigrate(results.getNewPids(), results.getNewReplicaPids());
    }

    void physicallyMigrate(Map<Integer, Integer> newPids, Map<Integer, Set<Integer>> newReplicaPids) {
        for(Integer uid : newPids.keySet()) {
            Integer newPid = newPids.get(uid);
            Set<Integer> newReplicas = newReplicaPids.get(uid);

            RepUser user = getUserMasterById(uid);
            Integer oldPid = user.getBasePid();
            Set<Integer> oldReplicas = user.getReplicaPids();

            if(!oldPid.equals(newPid)) {
                moveMasterAndInformReplicas(uid, user.getBasePid(), newPid);
                increaseTally(1);
            }

            if(!oldReplicas.equals(newReplicas)) {
                Set<Integer> replicasToAdd = new HashSet<>(newReplicas);
                replicasToAdd.removeAll(oldReplicas);
                for(Integer replicaPid : replicasToAdd) {
                    addReplica(user, replicaPid);
                }

                Set<Integer> replicasToRemove = new HashSet<>(oldReplicas);
                replicasToRemove.removeAll(newReplicas);
                for(Integer replicaPid : replicasToRemove) {
                    removeReplica(user, replicaPid);
                }
            }
        }
    }

}
