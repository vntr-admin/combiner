package io.vntr.sparmes;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesManager {
    private int minNumReplicas;

    private static final Long defaultStartingId = 1L;

    private SortedMap<Long, SparmesPartition> partitionIdToPartitionMap;

    private Map<Long, Long> userIdToMasterPartitionIdMap = new HashMap<Long, Long>();

    public SparmesManager(int minNumReplicas) {
        this.minNumReplicas = minNumReplicas;
        this.partitionIdToPartitionMap = new TreeMap<Long, SparmesPartition>();
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public SparmesPartition getPartitionById(Long id) {
        return partitionIdToPartitionMap.get(id);
    }

    public SparmesUser getUserMasterById(Long id) {
        Long partitionId = userIdToMasterPartitionIdMap.get(id);
        if (partitionId != null) {
            SparmesPartition partition = getPartitionById(partitionId);
            if (partition != null) {
                return partition.getMasterById(id);
            }
        }
        return null;
    }

    public int getNumUsers() {
        return userIdToMasterPartitionIdMap.size();
    }

    public Set<Long> getAllPartitionIds() {
        return partitionIdToPartitionMap.keySet();
    }

    public Set<Long> getAllUserIds() {
        return userIdToMasterPartitionIdMap.keySet();
    }

    public void addUser(User user) {
        Long masterPartitionId = getPartitionIdWithFewestMasters();

        SparmesUser spajaUser = new SparmesUser(user.getName(), user.getId());
        spajaUser.setMasterPartitionId(masterPartitionId);
        spajaUser.setPartitionId(masterPartitionId);

        addUser(spajaUser, masterPartitionId);

        for (Long id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(spajaUser, id);
        }
    }

    void addUser(SparmesUser user, Long masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        userIdToMasterPartitionIdMap.put(user.getId(), masterPartitionId);
    }

    public void removeUser(Long userId) {
        SparmesUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getMasterPartitionId()).removeMaster(userId);
        for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from userIdToMasterPartitionIdMap
        userIdToMasterPartitionIdMap.remove(userId);

        //Remove friendships
        for (Long friendId : user.getFriendIDs()) {
            SparmesUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Long friendReplicaPartitionId : friendMaster.getReplicaPartitionIds()) {
                SparmesPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(userId);
            }
        }
    }

    public Long addPartition() {
        Long newId = partitionIdToPartitionMap.isEmpty() ? defaultStartingId : partitionIdToPartitionMap.lastKey() + 1;
        addPartition(newId);
        return newId;
    }

    void addPartition(Long pid) {
        partitionIdToPartitionMap.put(pid, new SparmesPartition(pid));
    }

    public void removePartition(Long id) {
        partitionIdToPartitionMap.remove(id);
    }

    public void addReplica(SparmesUser user, Long destPid) {
        SparmesUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Long pid : user.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    SparmesUser addReplicaNoUpdates(SparmesUser user, Long destPid) {
        SparmesUser replica = user.clone();
        replica.setPartitionId(destPid);
        partitionIdToPartitionMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(SparmesUser user, Long removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Long currentReplicaPartitionId : user.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        partitionIdToPartitionMap.get(removalPartitionId).removeReplica(user.getId());
    }

    public void befriend(SparmesUser smallerUser, SparmesUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Long replicaPartitionId : smallerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Long replicaPartitionId : largerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(SparmesUser smallerUser, SparmesUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Long partitionId : smallerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Long partitionId : largerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void moveUser(SparmesUser user, Long destinationPartitionId, Set<Long> replicasToAddInDestinationPartition, Set<Long> replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        Long userId = user.getId();
        Long oldPartitionId = user.getMasterPartitionId();
        SparmesPartition oldPartition = partitionIdToPartitionMap.get(user.getMasterPartitionId());
        SparmesPartition newPartition = partitionIdToPartitionMap.get(destinationPartitionId);
        oldPartition.removeMaster(userId);
        newPartition.addMaster(user);
        userIdToMasterPartitionIdMap.put(userId, destinationPartitionId);

        user.setMasterPartitionId(destinationPartitionId);
        user.setPartitionId(destinationPartitionId);

        for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
            SparmesUser replica = partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setMasterPartitionId(destinationPartitionId);
        }

        //Step 2: add the necessary replicas
        for (Long friendId : user.getFriendIDs()) {
            if (userIdToMasterPartitionIdMap.get(friendId).equals(oldPartitionId)) {
                addReplica(user, oldPartitionId);
                break;
            }
        }

        for (Long friendToReplicateId : replicasToAddInDestinationPartition) {
            addReplica(getUserMasterById(friendToReplicateId), destinationPartitionId);
        }

        //Step 3: remove unnecessary replicas
        //Possibilities:
        // (1) replica of user in destinationPartition
        // (2) replicas of user's friends in oldPartition with no other purpose
        // (3) [the replica of the new friend that prompted this move should already be accounted for in (2)]

        if (user.getReplicaPartitionIds().contains(destinationPartitionId)) {
            if (user.getReplicaPartitionIds().size() > minNumReplicas) {
                removeReplica(user, destinationPartitionId);
            } else {
                //delete the replica in destinationPartition,  but add one in another partition that doesn't yet have one of this user
                addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user));
                removeReplica(user, destinationPartitionId);
            }
        }

        //delete the replica of the appropriate friends in oldPartition
        for (Long replicaIdToDelete : replicasToDeleteInSourcePartition) {
            removeReplica(getUserMasterById(replicaIdToDelete), oldPartitionId);
        }
    }

    public void promoteReplicaToMaster(Long userId, Long partitionId) {
        SparmesPartition partition = partitionIdToPartitionMap.get(partitionId);
        SparmesUser user = partition.getReplicaById(userId);
        user.setMasterPartitionId(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        userIdToMasterPartitionIdMap.put(userId, partitionId);

        for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
            SparmesUser replica = partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setMasterPartitionId(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }
    }

    Long getPartitionIdWithFewestMasters() {
        int minMasters = Integer.MAX_VALUE;
        Long minId = -1L;

        for (Long id : partitionIdToPartitionMap.keySet()) {
            int numMasters = getPartitionById(id).getNumMasters();
            if (numMasters < minMasters) {
                minMasters = numMasters;
                minId = id;
            }
        }

        return minId;
    }

    Long getRandomPartitionIdWhereThisUserIsNotPresent(SparmesUser user) {
        Set<Long> potentialReplicaLocations = new HashSet<Long>(partitionIdToPartitionMap.keySet());
        potentialReplicaLocations.remove(user.getMasterPartitionId());
        potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
        List<Long> list = new LinkedList<Long>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    Set<Long> getPartitionsToAddInitialReplicas(Long masterPartitionId) {
        List<Long> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<Long>(partitionIdToPartitionMap.keySet());
        partitionIdsAtWhichReplicasCanBeAdded.remove(masterPartitionId);
        return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
    }

    public Map<Long, Set<Long>> getPartitionToUserMap() {
        Map<Long, Set<Long>> map = new HashMap<Long, Set<Long>>();
        for (Long pid : partitionIdToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    Map<Long, Set<Long>> getPartitionToReplicasMap() {
        Map<Long, Set<Long>> map = new HashMap<Long, Set<Long>>();
        for (Long pid : partitionIdToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    public Long getEdgeCut() {
        long count = 0L;
        for (Long uid : userIdToMasterPartitionIdMap.keySet()) {
            SparmesUser user = getUserMasterById(uid);
            Long pid = user.getMasterPartitionId();
            for (Long friendId : user.getFriendIDs()) {
                if (!pid.equals(userIdToMasterPartitionIdMap.get(friendId))) {
                    count++;
                }
            }
        }
        return count / 2;
    }

    public Long getReplicationCount() {
        int count = 0;
        for(Long pid : getAllPartitionIds()) {
            count += getPartitionById(pid).getNumReplicas();
        }
        return (long) count;
    }
}
