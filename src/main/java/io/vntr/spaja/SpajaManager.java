package io.vntr.spaja;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SpajaManager {
    private int minNumReplicas;
    private int randomSampingSize;
    private float alpha;
    private float initialT;
    private float deltaT;

    private static final Integer defaultStartingId = 1;

    private SortedMap<Integer, SpajaPartition> partitionIdToPartitionMap;

    private NavigableMap<Integer, Integer> userIdToMasterPartitionIdMap = new TreeMap<Integer, Integer>();

    public SpajaManager(int minNumReplicas, float alpha, float initialT, float deltaT, int randomSampingSize) {
        this.minNumReplicas = minNumReplicas;
        this.partitionIdToPartitionMap = new TreeMap<Integer, SpajaPartition>();
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.randomSampingSize = randomSampingSize;
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public float getAlpha() {
        return alpha;
    }

    public float getInitialT() {
        return initialT;
    }

    public float getDeltaT() {
        return deltaT;
    }

    public int getRandomSampingSize() {
        return randomSampingSize;
    }

    public SpajaPartition getPartitionById(Integer id) {
        return partitionIdToPartitionMap.get(id);
    }

    public SpajaUser getUserMasterById(Integer id) {
        Integer partitionId = userIdToMasterPartitionIdMap.get(id);
        if (partitionId != null) {
            SpajaPartition partition = getPartitionById(partitionId);
            if (partition != null) {
                return partition.getMasterById(id);
            }
        }
        return null;
    }

    public Set<SpajaUser> getUserMastersById(Collection<Integer> ids) {
        Set<SpajaUser> users = new HashSet<SpajaUser>();
        for(Integer uid : ids) {
            users.add(getUserMasterById(uid));
        }
        return users;
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

    public void addUser(User user) {
        Integer masterPartitionId = getPartitionIdWithFewestMasters();

        SpajaUser spajaUser = new SpajaUser(user.getId(), alpha, minNumReplicas, this);
        spajaUser.setMasterPartitionId(masterPartitionId);
        spajaUser.setPartitionId(masterPartitionId);

        addUser(spajaUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(spajaUser, id);
        }
    }

    void addUser(SpajaUser user, Integer masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        userIdToMasterPartitionIdMap.put(user.getId(), masterPartitionId);
    }

    public void removeUser(Integer userId) {
        SpajaUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getMasterPartitionId()).removeMaster(userId);
        for (Integer replicaPartitionId : user.getReplicaPartitionIds()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from userIdToMasterPartitionIdMap
        userIdToMasterPartitionIdMap.remove(userId);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            SpajaUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Integer friendReplicaPartitionId : friendMaster.getReplicaPartitionIds()) {
                SpajaPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
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
        partitionIdToPartitionMap.put(pid, new SpajaPartition(pid));
    }

    public void removePartition(Integer id) {
        partitionIdToPartitionMap.remove(id);
    }

    public void addReplica(SpajaUser user, Integer destPid) {
        SpajaUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Integer pid : user.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    SpajaUser addReplicaNoUpdates(SpajaUser user, Integer destPid) {
        SpajaUser replica = user.clone();
        replica.setPartitionId(destPid);
        partitionIdToPartitionMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(SpajaUser user, Integer removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        partitionIdToPartitionMap.get(removalPartitionId).removeReplica(user.getId());
    }

    public void befriend(SpajaUser smallerUser, SpajaUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(SpajaUser smallerUser, SpajaUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void moveUser(SpajaUser user, Integer toPid, Set<Integer> replicateInDestPartition, Set<Integer> replicasToDeleteInSourcePartition) {
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

        for (Integer friendToReplicateId : replicateInDestPartition) {
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

    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        SpajaPartition partition = partitionIdToPartitionMap.get(partitionId);
        SpajaUser user = partition.getReplicaById(userId);
        user.setMasterPartitionId(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        userIdToMasterPartitionIdMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPartitionIds()) {
            SpajaUser replica = partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
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

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SpajaUser user) {
        Set<Integer> potentialReplicaLocations = new HashSet<Integer>(partitionIdToPartitionMap.keySet());
        potentialReplicaLocations.remove(user.getMasterPartitionId());
        potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
        List<Integer> list = new LinkedList<Integer>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    Set<Integer> getPartitionsToAddInitialReplicas(Integer masterPartitionId) {
        List<Integer> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<Integer>(partitionIdToPartitionMap.keySet());
        partitionIdsAtWhichReplicasCanBeAdded.remove(masterPartitionId);
        return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
    }

    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        Map<Integer, Set<Integer>> map = new HashMap<Integer, Set<Integer>>();
        for (Integer pid : partitionIdToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    Map<Integer, Set<Integer>> getPartitionToReplicasMap() {
        Map<Integer, Set<Integer>> map = new HashMap<Integer, Set<Integer>>();
        for (Integer pid : partitionIdToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    public Integer getEdgeCut() {
        int count = 0;
        for (Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            SpajaUser user = getUserMasterById(uid);
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

    void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        SpajaUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        userIdToMasterPartitionIdMap.put(uid, toPid);

        user.setMasterPartitionId(toPid);
        user.setPartitionId(toPid);

        for (Integer rPid : user.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(rPid).getReplicaById(uid).setMasterPartitionId(toPid);
        }
    }

    public void swap(Integer uid1, Integer uid2, SpajaBefriendingStrategy strategy) {
        SpajaUser u1 = getUserMasterById(uid1);
        SpajaUser u2 = getUserMasterById(uid2);

        Integer pid1 = u1.getMasterPartitionId();
        Integer pid2 = u2.getMasterPartitionId();

        //TODO: make sure this adds friend replicas on the new partition

        moveMasterAndInformReplicas(uid1, pid1, pid2);
        moveMasterAndInformReplicas(uid2, pid2, pid1);

        SwapChanges swapChanges = strategy.getSwapChanges(u1, u2);

        for(Integer uid : swapChanges.getAddToP1()) {
            addReplica(getUserMasterById(uid), pid1);
        }

        for(Integer uid : swapChanges.getAddToP2()) {
            addReplica(getUserMasterById(uid), pid2);
        }

        for(Integer uid : swapChanges.getRemoveFromP1()) {
            removeReplica(getUserMasterById(uid), pid1);
        }

        for(Integer uid : swapChanges.getRemoveFromP2()) {
            removeReplica(getUserMasterById(uid), pid2);
        }
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<Integer, Set<Integer>>();
        for(Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            friendships.put(uid, getUserMasterById(uid).getFriendIDs());
        }
        return friendships;
    }


    public int addUser() {
        int newUid = userIdToMasterPartitionIdMap.lastKey() + 1;
        addUser(new User(newUid));
        return newUid;
    }
}
