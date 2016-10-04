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
    private double alpha;
    private double initialT;
    private double deltaT;

    private static final Long defaultStartingId = 1L;

    private SortedMap<Long, SpajaPartition> partitionIdToPartitionMap;

    private Map<Long, Long> userIdToMasterPartitionIdMap = new HashMap<Long, Long>();

    public SpajaManager(int minNumReplicas, double alpha, double initialT, double deltaT, int randomSampingSize) {
        this.minNumReplicas = minNumReplicas;
        this.partitionIdToPartitionMap = new TreeMap<Long, SpajaPartition>();
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.randomSampingSize = randomSampingSize;
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public double getAlpha() {
        return alpha;
    }

    public double getInitialT() {
        return initialT;
    }

    public double getDeltaT() {
        return deltaT;
    }

    public int getRandomSampingSize() {
        return randomSampingSize;
    }

    public SpajaPartition getPartitionById(Long id) {
        return partitionIdToPartitionMap.get(id);
    }

    public SpajaUser getUserMasterById(Long id) {
        Long partitionId = userIdToMasterPartitionIdMap.get(id);
        if (partitionId != null) {
            SpajaPartition partition = getPartitionById(partitionId);
            if (partition != null) {
                return partition.getMasterById(id);
            }
        }
        return null;
    }

    public Set<SpajaUser> getUserMastersById(Collection<Long> ids) {
        Set<SpajaUser> users = new HashSet<SpajaUser>();
        for(Long uid : ids) {
            users.add(getUserMasterById(uid));
        }
        return users;
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

        SpajaUser spajaUser = new SpajaUser(user.getName(), user.getId(), alpha, minNumReplicas, this);
        spajaUser.setMasterPartitionId(masterPartitionId);
        spajaUser.setPartitionId(masterPartitionId);

        addUser(spajaUser, masterPartitionId);

        for (Long id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(spajaUser, id);
        }
    }

    void addUser(SpajaUser user, Long masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        userIdToMasterPartitionIdMap.put(user.getId(), masterPartitionId);
    }

    public void removeUser(Long userId) {
        SpajaUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getMasterPartitionId()).removeMaster(userId);
        for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from userIdToMasterPartitionIdMap
        userIdToMasterPartitionIdMap.remove(userId);

        //Remove friendships
        for (Long friendId : user.getFriendIDs()) {
            SpajaUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Long friendReplicaPartitionId : friendMaster.getReplicaPartitionIds()) {
                SpajaPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
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
        partitionIdToPartitionMap.put(pid, new SpajaPartition(pid));
    }

    public void removePartition(Long id) {
        partitionIdToPartitionMap.remove(id);
    }

    public void addReplica(SpajaUser user, Long destPid) {
        SpajaUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Long pid : user.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    SpajaUser addReplicaNoUpdates(SpajaUser user, Long destPid) {
        SpajaUser replica = user.clone();
        replica.setPartitionId(destPid);
        partitionIdToPartitionMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(SpajaUser user, Long removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Long currentReplicaPartitionId : user.getReplicaPartitionIds()) {
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

        for (Long replicaPartitionId : smallerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Long replicaPartitionId : largerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(SpajaUser smallerUser, SpajaUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Long partitionId : smallerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Long partitionId : largerUser.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void moveUser(SpajaUser user, Long toPid, Set<Long> replicateInDestPartition, Set<Long> replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        Long uid = user.getId();
        Long fromPid = user.getMasterPartitionId();

        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for (Long friendId : user.getFriendIDs()) {
            if (userIdToMasterPartitionIdMap.get(friendId).equals(fromPid)) {
                addReplica(user, fromPid);
                break;
            }
        }

        for (Long friendToReplicateId : replicateInDestPartition) {
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
        for (Long replicaIdToDelete : replicasToDeleteInSourcePartition) {
            removeReplica(getUserMasterById(replicaIdToDelete), fromPid);
        }
    }

    public void promoteReplicaToMaster(Long userId, Long partitionId) {
        SpajaPartition partition = partitionIdToPartitionMap.get(partitionId);
        SpajaUser user = partition.getReplicaById(userId);
        user.setMasterPartitionId(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        userIdToMasterPartitionIdMap.put(userId, partitionId);

        for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
            SpajaUser replica = partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
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

    Long getRandomPartitionIdWhereThisUserIsNotPresent(SpajaUser user) {
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
            SpajaUser user = getUserMasterById(uid);
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

    void moveMasterAndInformReplicas(Long uid, Long fromPid, Long toPid) {
        SpajaUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        userIdToMasterPartitionIdMap.put(uid, toPid);

        user.setMasterPartitionId(toPid);
        user.setPartitionId(toPid);

        for (Long rPid : user.getReplicaPartitionIds()) {
            partitionIdToPartitionMap.get(rPid).getReplicaById(uid).setMasterPartitionId(toPid);
        }
    }

    public void swap(Long uid1, Long uid2, SpajaBefriendingStrategy strategy) {
        SpajaUser u1 = getUserMasterById(uid1);
        SpajaUser u2 = getUserMasterById(uid2);

        Long pid1 = u1.getMasterPartitionId();
        Long pid2 = u2.getMasterPartitionId();

        moveMasterAndInformReplicas(uid1, pid1, pid2);
        moveMasterAndInformReplicas(uid2, pid2, pid1);

        SwapChanges swapChanges = strategy.getSwapChanges(u1, u2);

        for(Long uid : swapChanges.getAddToP1()) {
            addReplica(getUserMasterById(uid), pid1);
        }

        for(Long uid : swapChanges.getAddToP2()) {
            addReplica(getUserMasterById(uid), pid2);
        }

        for(Long uid : swapChanges.getRemoveFromP1()) {
            removeReplica(getUserMasterById(uid), pid1);
        }

        for(Long uid : swapChanges.getRemoveFromP2()) {
            removeReplica(getUserMasterById(uid), pid2);
        }
    }
}
