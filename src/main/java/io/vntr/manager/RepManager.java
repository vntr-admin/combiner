package io.vntr.manager;

import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

public class RepManager {
    private int minNumReplicas;

    private int nextPid = 1;
    private int nextUid = 1;

    private long migrationTally;
    private long logicalMigrationTally;
    private double logicalMigrationRatio;

    private Map<Integer, Partition> pMap;
    private Map<Integer, Integer> uMap;

    public RepManager(int minNumReplicas, double logicalMigrationRatio) {
        this.minNumReplicas = minNumReplicas;
        this.logicalMigrationRatio = logicalMigrationRatio;
        pMap = new HashMap<>();
        uMap = new HashMap<>();
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    Partition getPartitionById(Integer id) {
        return pMap.get(id);
    }

    public RepUser getUserMaster(Integer id) {
        Integer partitionId = uMap.get(id);
        if (partitionId != null) {
            Partition partition = getPartitionById(partitionId);
            if (partition != null) {
                return partition.getMasterById(id);
            }
        }
        return null;
    }

    public int getNumUsers() {
        return uMap.size();
    }

    public Set<Integer> getPids() {
        return pMap.keySet();
    }

    public Set<Integer> getUids() {
        return uMap.keySet();
    }

    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer masterPartitionId = getPidWithFewestMasters();

        RepUser RepUser = new RepUser(user.getId(), masterPartitionId);

        addUser(RepUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(RepUser, id);
        }
    }

    public void addUser(RepUser user, Integer masterPartitionId) {
        int uid = user.getId();
        getPartitionById(masterPartitionId).addMaster(user);
        uMap.put(uid, masterPartitionId);
        if(uid >= nextUid) {
            nextUid = uid + 1;
        }
    }

    public void removeUser(Integer uid) {
        RepUser user = getUserMaster(uid);

        //Remove user from relevant partitions
        getPartitionById(user.getBasePid()).removeMaster(uid);
        for (Integer replicaPartitionId : user.getReplicaPids()) {
            getPartitionById(replicaPartitionId).removeReplica(uid);
        }

        //Remove user from uMap
        uMap.remove(uid);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            RepUser friendMaster = getUserMaster(friendId);
            friendMaster.unfriend(uid);

            for (Integer friendReplicaPartitionId : friendMaster.getReplicaPids()) {
                Partition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(uid);
            }
        }
    }

    public Integer addPartition() {
        Integer newId = nextPid;
        addPartition(newId);
        return newId;
    }

    public void addPartition(Integer pid) {
        pMap.put(pid, new Partition(pid));
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    public void removePartition(Integer id) {
        pMap.remove(id);
    }

    public void addReplica(RepUser user, Integer destPid) {
        RepUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Integer pid : user.getReplicaPids()) {
            pMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    public RepUser addReplicaNoUpdates(RepUser user, Integer destPid) {
        RepUser replica = user.dupe();
        pMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(RepUser user, Integer removalPid) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
            Partition p = pMap.get(currentReplicaPartitionId);
            RepUser r = p.getReplicaById(user.getId());
            r.removeReplicaPartitionId(removalPid);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPid);

        //Actually remove the replica from the partition itself
        pMap.get(removalPid).removeReplica(user.getId());
    }

    public void moveUser(RepUser user, Integer toPid, Set<Integer> replicateInDestinationPartition, Set<Integer> replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        Integer uid = user.getId();
        Integer fromPid = user.getBasePid();

        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for (Integer friendId : user.getFriendIDs()) {
            if (uMap.get(friendId).equals(fromPid)) {
                addReplica(user, fromPid);
                break;
            }
        }

        for (Integer friendToReplicateId : replicateInDestinationPartition) {
            addReplica(getUserMaster(friendToReplicateId), toPid);
        }

        //Step 3: remove unnecessary replicas
        //Possibilities:
        // (1) replica of user in destinationPartition
        // (2) replicas of user's friends in oldPartition with no other purpose
        // (3) [the replica of the new friend that prompted this move should already be accounted for in (2)]

        if (user.getReplicaPids().contains(toPid)) {
            if (user.getReplicaPids().size() <= minNumReplicas) {
                //add one in another partition that doesn't yet have one of this user
                addReplica(user, getRandomPidWhereThisUserIsNotPresent(user));
            }
            removeReplica(user, toPid);
        }

        //delete the replica of the appropriate friends in oldPartition
        for (Integer replicaIdToDelete : replicasToDeleteInSourcePartition) {
            removeReplica(getUserMaster(replicaIdToDelete), fromPid);
        }

        increaseTally(1);
    }

    public void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        RepUser user = getUserMaster(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        uMap.put(uid, toPid);

        user.setBasePid(toPid);

        for (Integer rPid : user.getReplicaPids()) {
            pMap.get(rPid).getReplicaById(uid).setBasePid(toPid);
        }
    }

    public void befriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPids()) {
            pMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPids()) {
            pMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPids()) {
            pMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPids()) {
            pMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        Partition partition = pMap.get(partitionId);
        RepUser user = partition.getReplicaById(userId);
        user.setBasePid(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        uMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPids()) {
            RepUser replica = pMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setBasePid(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }

        //Add replicas of friends in pid if they don't already exist
        for(int friendId : user.getFriendIDs()) {
            if(!partition.getIdsOfMasters().contains(friendId) && !partition.getIdsOfReplicas().contains(friendId)) {
                addReplica(getUserMaster(friendId), partitionId);
            }
        }
    }

    public Integer getPidWithFewestMasters() {
        int minMasters = Integer.MAX_VALUE;
        Integer minId = -1;

        for (Integer id : pMap.keySet()) {
            int numMasters = getPartitionById(id).getNumMasters();
            if (numMasters < minMasters) {
                minMasters = numMasters;
                minId = id;
            }
        }

        return minId;
    }

    public Integer getRandomPidWhereThisUserIsNotPresent(RepUser user) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(pMap.keySet());
        potentialReplicaLocations.remove(user.getBasePid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        List<Integer> list = new LinkedList<>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    public Set<Integer> getPartitionsToAddInitialReplicas(Integer masterPid) {
        List<Integer> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<>(pMap.keySet());
        partitionIdsAtWhichReplicasCanBeAdded.remove(masterPid);
        return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
    }

    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : pMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    public Map<Integer, Set<Integer>> getPartitionToReplicasMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : pMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    public Integer getEdgeCut() {
        int count = 0;
        for (Integer uid : uMap.keySet()) {
            RepUser user = getUserMaster(uid);
            Integer pid = user.getBasePid();
            for (Integer friendId : user.getFriendIDs()) {
                if (pid < uMap.get(friendId)) {
                    count++;
                }
            }
        }
        return count;
    }

    public Integer getReplicationCount() {
        int count = 0;
        for(Integer pid : getPids()) {
            count += getPartitionById(pid).getNumReplicas();
        }
        return count;
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : uMap.keySet()) {
            friendships.put(uid, getUserMaster(uid).getFriendIDs());
        }
        return friendships;
    }

    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * logicalMigrationTally);
    }

    public void increaseTally(int amount) {
        migrationTally += amount;
    }

    public void increaseTallyLogical(int amount) {
        logicalMigrationTally += amount;
    }

    @Override
    public String toString() {
        return "minNumReplicas:" + minNumReplicas + "|#U:" + getNumUsers() + "|#P:" + pMap.size();
    }

    public void checkValidity() {

        //Check masters
        for(Integer uid : uMap.keySet()) {
            Integer observedMasterPid = null;
            for(Integer pid : pMap.keySet()) {
                if(pMap.get(pid).getIdsOfMasters().contains(uid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot be in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must be in some partition");
            }
            if(!observedMasterPid.equals(uMap.get(uid))) {
                throw new RuntimeException("Mismatch between uMap's location of user and partition's");
            }
            if(!observedMasterPid.equals(getUserMaster(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }

            //TODO: should we check the logical partitions?
        }

        //check replicas
        for(Integer uid : uMap.keySet()) {
            RepUser user = getUserMaster(uid);
            Set<Integer> observedReplicaPids = new HashSet<>();
            for(Integer pid : pMap.keySet()) {
                if(pMap.get(pid).getIdsOfReplicas().contains(uid)) {
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

    public Set<Integer> getMastersOnPartition(int pid) {
        return getPartitionById(pid).getIdsOfMasters();
    }

    public Set<Integer> getReplicasOnPartition(int pid) {
        return getPartitionById(pid).getIdsOfReplicas();
    }

    public RepUser getReplicaOnPartition(int uid, int pid) {
        return getPartitionById(pid).getReplicaById(uid);
    }

    public int getNumMastersOnPartition(int pid) {
        return getPartitionById(pid).getNumMasters();
    }

    public int getNumReplicasOnPartition(int pid) {
        return getPartitionById(pid).getNumReplicas();
    }

    static class Partition {
        private Map<Integer, RepUser> idToMasterMap = new HashMap<>();
        private Map<Integer, RepUser> idToReplicaMap = new HashMap<>();
        private Integer id;

        Partition(Integer id) {
            this.id = id;
        }

        void addMaster(RepUser user) {
            idToMasterMap.put(user.getId(), user);
        }

        User removeMaster(Integer id) {
            return idToMasterMap.remove(id);
        }

        void addReplica(RepUser user) {
            idToReplicaMap.put(user.getId(), user);
        }

        User removeReplica(Integer id) {
            return idToReplicaMap.remove(id);
        }

        RepUser getMasterById(Integer userId) {
            return idToMasterMap.get(userId);
        }

        RepUser getReplicaById(Integer userId) {
            return idToReplicaMap.get(userId);
        }

        int getNumMasters() {
            return idToMasterMap.size();
        }

        int getNumReplicas() {
            return idToReplicaMap.size();
        }

        Set<Integer> getIdsOfMasters() {
            return idToMasterMap.keySet();
        }

        public Set<Integer> getIdsOfReplicas() {
            return idToReplicaMap.keySet();
        }

        public Integer getId() {
            return id;
        }

    }
}
