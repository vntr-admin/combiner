package io.vntr.spar;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

public class SparManager {
    private int minNumReplicas;

    private static final Integer defaultStartingId = 1;
    private int nextPid = 1;
    private int nextUid = 1;

    private long migrationTally;

    private Map<Integer, SparPartition> partitionIdToPartitionMap;

    private Map<Integer, Integer> userIdToMasterPartitionIdMap = new HashMap<>();

    public SparManager(int minNumReplicas) {
        this.minNumReplicas = minNumReplicas;
        this.partitionIdToPartitionMap = new HashMap<>();
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public SparPartition getPartitionById(Integer id) {
        return partitionIdToPartitionMap.get(id);
    }

    public SparUser getUserMasterById(Integer id) {
        Integer partitionId = userIdToMasterPartitionIdMap.get(id);
        if (partitionId != null) {
            SparPartition partition = getPartitionById(partitionId);
            if (partition != null) {
                return partition.getMasterById(id);
            }
        }
        return null;
    }

    public int getNumUsers() {
        return userIdToMasterPartitionIdMap.size();
    }

    public Set<Integer> getAllPartitionIds() {
        return partitionIdToPartitionMap.keySet();
    }

    public Set<Integer> getAllUserIds() {
        return userIdToMasterPartitionIdMap.keySet();
    }

    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer masterPartitionId = getPartitionIdWithFewestMasters();

        int uid = user.getId();
        SparUser sparUser = new SparUser(uid);
        sparUser.setMasterPid(masterPartitionId);

        addUser(sparUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(sparUser, id);
        }
    }

    void addUser(SparUser user, Integer masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        userIdToMasterPartitionIdMap.put(user.getId(), masterPartitionId);
        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }
    }

    public void removeUser(Integer userId) {
        SparUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getMasterPid()).removeMaster(userId);
        for (Integer replicaPartitionId : user.getReplicaPids()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from userIdToMasterPartitionIdMap
        userIdToMasterPartitionIdMap.remove(userId);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            SparUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Integer friendReplicaPartitionId : friendMaster.getReplicaPids()) {
                SparPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(userId);
            }
        }
    }

    public Integer addPartition() {
        Integer newId = nextPid;
        addPartition(newId);
        return newId;
    }

    void addPartition(Integer pid) {
        partitionIdToPartitionMap.put(pid, new SparPartition(pid));
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    public void removePartition(Integer id) {
        partitionIdToPartitionMap.remove(id);
    }

    public void addReplica(SparUser user, Integer destPid) {
        SparUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Integer pid : user.getReplicaPids()) {
            partitionIdToPartitionMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    SparUser addReplicaNoUpdates(SparUser user, Integer destPid) {
        SparUser replica = user.clone();
        partitionIdToPartitionMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(SparUser user, Integer removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
            SparPartition p = partitionIdToPartitionMap.get(currentReplicaPartitionId);
            SparUser r = p.getReplicaById(user.getId());
            r.removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        partitionIdToPartitionMap.get(removalPartitionId).removeReplica(user.getId());
    }

    public void moveUser(SparUser user, Integer toPid, Set<Integer> replicateInDestinationPartition, Set<Integer> replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        Integer uid = user.getId();
        Integer fromPid = user.getMasterPid();

        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for (Integer friendId : user.getFriendIDs()) {
            if (userIdToMasterPartitionIdMap.get(friendId).equals(fromPid)) {
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

        incrementMigrationTally();
    }

    void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        SparUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        userIdToMasterPartitionIdMap.put(uid, toPid);

        user.setMasterPid(toPid);

        for (Integer rPid : user.getReplicaPids()) {
            partitionIdToPartitionMap.get(rPid).getReplicaById(uid).setMasterPid(toPid);
        }
    }

    public void befriend(SparUser smallerUser, SparUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPids()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPids()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(SparUser smallerUser, SparUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPids()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPids()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        SparPartition partition = partitionIdToPartitionMap.get(partitionId);
        SparUser user = partition.getReplicaById(userId);
        user.setMasterPid(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        userIdToMasterPartitionIdMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPids()) {
            SparUser replica = partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setMasterPid(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }

        //Add replicas of friends in partitionId if they don't already exist
        for(int friendId : user.getFriendIDs()) {
            if(!partition.getIdsOfMasters().contains(friendId) && !partition.getIdsOfReplicas().contains(friendId)) {
                addReplica(getUserMasterById(friendId), partitionId);
            }
        }
    }

    Integer getPartitionIdWithFewestMasters() {
        int minMasters = Integer.MAX_VALUE;
        Integer minId = -1;

        for (Integer id : partitionIdToPartitionMap.keySet()) {
            int numMasters = getPartitionById(id).getNumMasters();
            if (numMasters < minMasters) {
                minMasters = numMasters;
                minId = id;
            }
        }

        return minId;
    }

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SparUser user) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(partitionIdToPartitionMap.keySet());
        potentialReplicaLocations.remove(user.getMasterPid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        List<Integer> list = new LinkedList<>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    Set<Integer> getPartitionsToAddInitialReplicas(Integer masterPartitionId) {
        List<Integer> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<>(partitionIdToPartitionMap.keySet());
        partitionIdsAtWhichReplicasCanBeAdded.remove(masterPartitionId);
        return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
    }

    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : partitionIdToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    Map<Integer, Set<Integer>> getPartitionToReplicasMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : partitionIdToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    public Integer getEdgeCut() {
        int count = 0;
        for (Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            SparUser user = getUserMasterById(uid);
            Integer pid = user.getMasterPid();
            for (Integer friendId : user.getFriendIDs()) {
                if (!pid.equals(userIdToMasterPartitionIdMap.get(friendId))) {
                    count++;
                }
            }
        }
        return count / 2;
    }

    public Integer getReplicationCount() {
        int count = 0;
        for(Integer pid : getAllPartitionIds()) {
            count += getPartitionById(pid).getNumReplicas();
        }
        return count;
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            friendships.put(uid, getUserMasterById(uid).getFriendIDs());
        }
        return friendships;
    }

    public long getMigrationTally() {
        return migrationTally;
    }

    void incrementMigrationTally() {
        migrationTally++;
    }

    @Override
    public String toString() {
        return "minNumReplicas:" + minNumReplicas + "|#U:" + getNumUsers() + "|#P:" + partitionIdToPartitionMap.size();
    }

    void checkValidity() {

        //Check masters
        for(Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            Integer observedMasterPid = null;
            for(Integer pid : partitionIdToPartitionMap.keySet()) {
                if(partitionIdToPartitionMap.get(pid).getIdsOfMasters().contains(uid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot be in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must be in some partition");
            }
            if(!observedMasterPid.equals(userIdToMasterPartitionIdMap.get(uid))) {
                throw new RuntimeException("Mismatch between uMap's location of user and partition's");
            }
            if(!observedMasterPid.equals(getUserMasterById(uid).getMasterPid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }

            //TODO: should we check the logical partitions?
        }

        //check replicas
        for(Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            SparUser user = getUserMasterById(uid);
            Set<Integer> observedReplicaPids = new HashSet<>();
            for(Integer pid : partitionIdToPartitionMap.keySet()) {
                if(partitionIdToPartitionMap.get(pid).getIdsOfReplicas().contains(uid)) {
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

}