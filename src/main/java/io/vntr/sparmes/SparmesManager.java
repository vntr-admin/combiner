package io.vntr.sparmes;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesManager {
    private int minNumReplicas;
    private float gamma;

    private static final Integer defaultStartingId = 1;

    private SortedMap<Integer, SparmesPartition> pMap;

    private Map<Integer, Integer> uMap = new HashMap<Integer, Integer>();

    private SparmesBefriendingStrategy sparmesBefriendingStrategy;

    public SparmesManager(int minNumReplicas, float gamma) {
        this.minNumReplicas = minNumReplicas;
        this.gamma = gamma;
        this.pMap = new TreeMap<Integer, SparmesPartition>();
        this.sparmesBefriendingStrategy = new SparmesBefriendingStrategy(this);
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public SparmesBefriendingStrategy getSparmesBefriendingStrategy() {
        return sparmesBefriendingStrategy;
    }

    public SparmesPartition getPartitionById(Integer id) {
        return pMap.get(id);
    }

    public SparmesUser getUserMasterById(Integer id) {
        Integer partitionId = uMap.get(id);
        if (partitionId != null) {
            SparmesPartition partition = getPartitionById(partitionId);
            if (partition != null) {
                return partition.getMasterById(id);
            }
        }
        return null;
    }

    public int getNumUsers() {
        return uMap.size();
    }

    public Set<Integer> getAllPartitionIds() {
        return pMap.keySet();
    }

    public Set<Integer> getAllUserIds() {
        return uMap.keySet();
    }

    public void addUser(User user) {
        Integer masterPartitionId = getPartitionIdWithFewestMasters();

        SparmesUser spajaUser = new SparmesUser(user.getId(), masterPartitionId, gamma, this);

        addUser(spajaUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(spajaUser, id);
        }
    }

    void addUser(SparmesUser user, Integer masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        uMap.put(user.getId(), masterPartitionId);
    }

    public void removeUser(Integer userId) {
        SparmesUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getMasterPartitionId()).removeMaster(userId);
        for (Integer replicaPartitionId : user.getReplicaPartitionIds()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from uMap
        uMap.remove(userId);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            SparmesUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Integer friendReplicaPartitionId : friendMaster.getReplicaPartitionIds()) {
                SparmesPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(userId);
            }
        }
    }

    public Integer addPartition() {
        Integer newId = pMap.isEmpty() ? defaultStartingId : pMap.lastKey() + 1;
        addPartition(newId);
        return newId;
    }

    void addPartition(Integer pid) {
        pMap.put(pid, new SparmesPartition(pid, gamma, this));
    }

    public void removePartition(Integer id) {
        pMap.remove(id);
    }

    public void addReplica(SparmesUser user, Integer destPid) {
        SparmesUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Integer pid : user.getReplicaPartitionIds()) {
            pMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    SparmesUser addReplicaNoUpdates(SparmesUser user, Integer destPid) {
        SparmesUser replica = user.clone();
        replica.setPartitionId(destPid);
        pMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(SparmesUser user, Integer removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPartitionIds()) {
            pMap.get(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        pMap.get(removalPartitionId).removeReplica(user.getId());
    }

    public void befriend(SparmesUser smallerUser, SparmesUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPartitionIds()) {
            pMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPartitionIds()) {
            pMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(SparmesUser smallerUser, SparmesUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPartitionIds()) {
            pMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPartitionIds()) {
            pMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void moveUser(SparmesUser user, Integer toPid, Set<Integer> replicateInDestinationPartition, Set<Integer> replicasToDeleteInSourcePartition) {
        Integer uid = user.getId();
        Integer fromPid = user.getMasterPartitionId();

        //Step 1: move the actual user
        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for (Integer friendId : user.getFriendIDs()) {
            if (uMap.get(friendId).equals(fromPid)) {
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

    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        SparmesPartition partition = pMap.get(partitionId);
        SparmesUser user = partition.getReplicaById(userId);
        user.setMasterPartitionId(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        uMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPartitionIds()) {
            SparmesUser replica = pMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setMasterPartitionId(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }
    }

    Integer getPartitionIdWithFewestMasters() {
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

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SparmesUser user) {
        Set<Integer> potentialReplicaLocations = new HashSet<Integer>(pMap.keySet());
        potentialReplicaLocations.remove(user.getMasterPartitionId());
        potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
        List<Integer> list = new LinkedList<Integer>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    Set<Integer> getPartitionsToAddInitialReplicas(Integer masterPartitionId) {
        List<Integer> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<Integer>(pMap.keySet());
        partitionIdsAtWhichReplicasCanBeAdded.remove(masterPartitionId);
        return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
    }

    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        Map<Integer, Set<Integer>> map = new HashMap<Integer, Set<Integer>>();
        for (Integer pid : pMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    Map<Integer, Set<Integer>> getPartitionToReplicasMap() {
        Map<Integer, Set<Integer>> map = new HashMap<Integer, Set<Integer>>();
        for (Integer pid : pMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    public Integer getEdgeCut() {
        int count = 0;
        for (Integer uid : uMap.keySet()) {
            SparmesUser user = getUserMasterById(uid);
            Integer pid = user.getMasterPartitionId();
            for (Integer friendId : user.getFriendIDs()) {
                if (!pid.equals(uMap.get(friendId))) {
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
        return (int) count;
    }

    void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        SparmesUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        uMap.put(uid, toPid);

        user.setMasterPartitionId(toPid);
        user.setPartitionId(toPid);

        for (Integer rPid : user.getReplicaPartitionIds()) {
            pMap.get(rPid).getReplicaById(uid).setMasterPartitionId(toPid);
        }
    }

    public void repartition() {
        int k = 3; //TODO: set this intelligently
        for (SparmesPartition p : pMap.values()) {
            p.resetLogicalUsers();
        }

        boolean stoppingCondition = false;
        while (!stoppingCondition) {
            boolean changed = false;
            changed |= performStage(true,  k);
            changed |= performStage(false, k);
            stoppingCondition = !changed;
        }

        Map<Integer, Integer> usersWhoMoved = new HashMap<Integer, Integer>();
        for (SparmesPartition p : pMap.values()) {
            Set<Integer> moved = p.physicallyMigrateCopy();
            for(Integer uid : moved) {
                usersWhoMoved.put(uid, p.getId());
            }
        }

        for (SparmesPartition p : pMap.values()) {
            p.physicallyMigrateDelete();
        }

        uMap.putAll(usersWhoMoved);
    }

    boolean performStage(boolean firstStage, int k) {
        boolean changed = false;
        Map<Integer, Set<Target>> stageTargets = new HashMap<Integer, Set<Target>>();
        for (SparmesPartition p : pMap.values()) {
            Set<Target> targets = p.getCandidates(firstStage, k);
            stageTargets.put(p.getId(), targets);
            changed |= !targets.isEmpty();
        }

        for(Integer pid : pMap.keySet()) {
            for(Target target : stageTargets.get(pid)) {
                migrateLogically(target);
            }
        }

        updateLogicalUsers();

        return changed;
    }

    private void addLogicalReplica(Integer uid, Integer pid) {
        getUserMasterById(uid).addLogicalPartitionId(pid);
        getPartitionById(pid).addLogicalReplicaId(uid);
    }

    private void removeLogicalReplica(Integer uid, Integer pid) {
        getUserMasterById(uid).removeLogicalPartitionId(pid);
        getPartitionById(pid).removeLogicalReplicaId(uid);
    }

    void migrateLogically(Target t) {
        SparmesPartition oldPart = getPartitionById(t.oldPid);
        SparmesPartition newPart = getPartitionById(t.newPid);
        SparmesUser user = getUserMasterById(t.uid);

        //Add the actual user
        user.setLogicalPid(t.newPid);
        oldPart.removeLogicalUser(t.uid);
        newPart.addLogicalUser(user.getLogicalUser(false));

        //Add replicas as necessary
        //First, replicate user in old partition if necessary
        for(Integer friendId : user.getFriendIDs()) {
            if(getUserMasterById(friendId).getLogicalPid().equals(t.oldPid)) {
                addLogicalReplica(t.uid, t.oldPid);
                break;
            }
        }

        //Second, replicate friends in new partition if they aren't there already
        for(Integer friendId : user.getFriendIDs()) {
            SparmesUser friend = getUserMasterById(friendId);
            if(!friend.getLogicalPid().equals(t.newPid) && !friend.getLogicalPartitionIds().contains(t.newPid)) {
                addLogicalReplica(friendId, t.newPid);
            }
        }

        //Remove replicas as allowed
        //First, remove user replica from new partition if one exists
        if(newPart.getLogicalReplicaIds().contains(t.uid)) {
            removeLogicalReplica(t.uid, t.newPid);
        }

        //Second, if we've violated k-constraints, choose another partition at random and replicate this user there
        if(user.getLogicalPartitionIds().size() < minNumReplicas) {
            Set<Integer> potentialReplicaLocations = new HashSet<Integer>(pMap.keySet());
            potentialReplicaLocations.remove(user.getLogicalPid());
            potentialReplicaLocations.removeAll(user.getLogicalPartitionIds());
            List<Integer> list = new LinkedList<Integer>(potentialReplicaLocations);
            Integer newReplicaPid = list.get((int) (list.size() * Math.random()));
            addLogicalReplica(t.uid, newReplicaPid);
        }

        //Third, remove friends replicas from old partition if they weren't being used for any other reason and don't violate k-replication
        Set<Integer> friendReplicasToRemove = new HashSet<Integer>(user.getFriendIDs());
        friendReplicasToRemove.retainAll(oldPart.getLogicalReplicaIds());
        for(Integer friendId : friendReplicasToRemove) {
            SparmesUser friend = getUserMasterById(friendId);
            Set<Integer> friendsOfFriend = new HashSet<Integer>(friend.getFriendIDs());
            friendsOfFriend.retainAll(oldPart.getLogicalUserIds());
            if(friendsOfFriend.isEmpty() && friend.getLogicalPartitionIds().size() > minNumReplicas) {
                oldPart.removeLogicalReplicaId(friendId);
            }
        }
    }

    void updateLogicalUsers() {
        Map<Integer, Integer> pToWeight = getPToWeight(false);
        int totalWeight = 0;
        for(Integer pWeight: pToWeight.values()) {
            totalWeight += pWeight;
        }
        for(SparmesPartition p : pMap.values()) {
            for(Integer logicalUid : p.getLogicalUserIds()) {
                SparmesUser user = getUserMasterById(logicalUid);
                Map<Integer, Integer> updatedFriendCounts = user.getPToFriendCountLogical();
                boolean replicateInSourcePartition = user.shouldReplicateInSourcePartitionLogical();
                int numFriendsToDeleteInCurrentPartition = user.getNumFriendsToDeleteInCurrentPartitionLogical();
                Map<Integer, Integer> friendsToAddInEachPartition = user.getFriendsToAddInEachPartitionLogical();
                LogicalUser luser = new LogicalUser(user.getId(), user.getLogicalPid(), gamma, updatedFriendCounts, pToWeight, user.getLogicalPartitionIds(), friendsToAddInEachPartition, numFriendsToDeleteInCurrentPartition, replicateInSourcePartition, totalWeight);
                p.addLogicalUser(luser);
            }
        }
    }

    Map<Integer, Integer> getPToWeight(boolean determineWeightsFromPhysicalPartitions) {
        Map<Integer, Integer> pToWeight = new HashMap<Integer, Integer>();
        for(Integer partitionId : getAllPartitionIds()) {
            int pWeight;
            if(determineWeightsFromPhysicalPartitions) {
                pWeight = getPartitionById(partitionId).getNumMasters();
            }
            else {
                pWeight = getPartitionById(partitionId).getNumLogicalUsers();
            }
            pToWeight.put(partitionId, (int) pWeight);
        }
        return pToWeight;
    }

}
