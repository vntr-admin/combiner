package io.vntr.replicadummy;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class ReplicaDummyManager {

    private int minNumReplicas;

    private static final Integer defaultStartingId = 1;

    private SortedMap<Integer, ReplicaDummyPartition> partitionIdToPartitionMap;

    private NavigableMap<Integer, Integer> userIdToMasterPartitionIdMap = new TreeMap<>();

    public ReplicaDummyManager(int minNumReplicas) {
        this.minNumReplicas = minNumReplicas;
        partitionIdToPartitionMap = new TreeMap<>();
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public ReplicaDummyPartition getPartitionById(Integer id) {
        return partitionIdToPartitionMap.get(id);
    }

    public ReplicaDummyUser getUserMasterById(Integer id) {
        Integer partitionId = userIdToMasterPartitionIdMap.get(id);
        if (partitionId != null) {
            ReplicaDummyPartition partition = getPartitionById(partitionId);
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
        int newUid = userIdToMasterPartitionIdMap.lastKey() + 1;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer masterPartitionId = getPartitionIdWithFewestMasters();

        ReplicaDummyUser ReplicaDummyUser = new ReplicaDummyUser(user.getId());
        ReplicaDummyUser.setMasterPartitionId(masterPartitionId);
        ReplicaDummyUser.setPartitionId(masterPartitionId);

        addUser(ReplicaDummyUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(ReplicaDummyUser, id);
        }
    }

    void addUser(ReplicaDummyUser user, Integer masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        userIdToMasterPartitionIdMap.put(user.getId(), masterPartitionId);
    }

    public void removeUser(Integer userId) {
        ReplicaDummyUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getMasterPartitionId()).removeMaster(userId);
        for (Integer replicaPartitionId : user.getReplicaPartitionIds()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from userIdToMasterPartitionIdMap
        userIdToMasterPartitionIdMap.remove(userId);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            ReplicaDummyUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Integer friendReplicaPartitionId : friendMaster.getReplicaPartitionIds()) {
                ReplicaDummyPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(userId);
            }
        }
    }

    public Integer addPartition() {
        Integer newId = partitionIdToPartitionMap.isEmpty() ? defaultStartingId : partitionIdToPartitionMap.lastKey() + 1;
        addPartition(newId);
        return newId;
    }

    void addPartition(Integer pid) {
        partitionIdToPartitionMap.put(pid, new ReplicaDummyPartition(pid));
    }

    public void removePartition(Integer id) {
        partitionIdToPartitionMap.remove(id);
    }

    public void addReplica(ReplicaDummyUser user, Integer destPid) {
        ReplicaDummyUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Integer pid : user.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    ReplicaDummyUser addReplicaNoUpdates(ReplicaDummyUser user, Integer destPid) {
        ReplicaDummyUser replica = user.clone();
        replica.setPartitionId(destPid);
        partitionIdToPartitionMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(ReplicaDummyUser user, Integer removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPartitionIds()) {
            ReplicaDummyPartition p = partitionIdToPartitionMap.get(currentReplicaPartitionId);
            ReplicaDummyUser r = p.getReplicaById(user.getId());
            r.removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        partitionIdToPartitionMap.get(removalPartitionId).removeReplica(user.getId());
    }

    public void moveUser(ReplicaDummyUser user, Integer toPid, Set<Integer> replicateInDestinationPartition, Set<Integer> replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        Integer uid = user.getId();
        Integer fromPid = user.getMasterPartitionId();

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

        if (user.getReplicaPartitionIds().contains(toPid)) {
            if (user.getReplicaPartitionIds().size() <= minNumReplicas) {
                //add one in another partition that doesn't yet have one of this user
                addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user));
            }
            removeReplica(user, toPid);
        }

        //delete the replica of the appropriate friends in oldPartition
        for (Integer replicaIdToDelete : replicasToDeleteInSourcePartition) {
            removeReplica(getUserMasterById(replicaIdToDelete), fromPid);
        }
    }

    void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        ReplicaDummyUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        userIdToMasterPartitionIdMap.put(uid, toPid);

        user.setMasterPartitionId(toPid);
        user.setPartitionId(toPid);

        for (Integer rPid : user.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(rPid).getReplicaById(uid).setMasterPartitionId(toPid);
        }
    }

    public void befriend(ReplicaDummyUser smallerUser, ReplicaDummyUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(ReplicaDummyUser smallerUser, ReplicaDummyUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        ReplicaDummyPartition partition = partitionIdToPartitionMap.get(partitionId);
        ReplicaDummyUser user = partition.getReplicaById(userId);
        user.setMasterPartitionId(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        userIdToMasterPartitionIdMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPartitionIds()) {
            ReplicaDummyUser replica = partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setMasterPartitionId(partitionId);
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

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(ReplicaDummyUser user) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(partitionIdToPartitionMap.keySet());
        potentialReplicaLocations.remove(user.getMasterPartitionId());
        potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
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
            ReplicaDummyUser user = getUserMasterById(uid);
            Integer pid = user.getMasterPartitionId();
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

    @Override
    public String toString() {
        return "minNumReplicas:" + minNumReplicas + "|#U:" + getNumUsers() + "|#P:" + partitionIdToPartitionMap.size();
    }

    void checkValidity() {
        boolean valid = true;
        for(Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            ReplicaDummyUser user = getUserMasterById(uid);
            Integer observedMasterPid = null;
            Set<Integer> replicaPidsFromPartitions = new HashSet<>();
            for(Integer pid : partitionIdToPartitionMap.keySet()) {
                ReplicaDummyPartition p = partitionIdToPartitionMap.get(pid);
                if(p.getIdsOfReplicas().contains(uid)) {
                    replicaPidsFromPartitions.add(pid);
                }
                if(p.getIdsOfMasters().contains(observedMasterPid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot have master in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must have a master in some partition");
            }
            if(!observedMasterPid.equals(user.getMasterPartitionId())) {
                throw new RuntimeException("Mismatch between user's master PID and system's");
            }
            if(replicaPidsFromPartitions.size() < minNumReplicas) {
                throw new RuntimeException("Insufficient replicas");
            }
            if(!replicaPidsFromPartitions.equals(user.getReplicaPartitionIds())) {
                throw new RuntimeException("Mismatch between user's replica PIDs and system's");
            }
        }
    }
}
