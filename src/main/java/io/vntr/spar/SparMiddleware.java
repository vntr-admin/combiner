package io.vntr.spar;

import java.util.*;

import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;

public class SparMiddleware implements IMiddlewareAnalyzer {
    SparManager manager;
    private SparBefriendingStrategy sparBefriendingStrategy;
    private SparMigrationStrategy sparMigrationStrategy;

    public SparMiddleware(int minNumReplicas) {
        manager = new SparManager(minNumReplicas);
        sparBefriendingStrategy = new SparBefriendingStrategy(manager);
        sparMigrationStrategy = new SparMigrationStrategy(manager);
    }

    public SparMiddleware(SparManager manager) {
        this.manager = manager;
        sparBefriendingStrategy = new SparBefriendingStrategy(manager);
        sparMigrationStrategy = new SparMigrationStrategy(manager);
    }

    @Override
    public int addUser() {
        return manager.addUser();
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
    }

    @Override
    public void removeUser(Integer userId) {
        manager.removeUser(userId);
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        SparUser smallerUser = manager.getUserMasterById(smallerUserId);
        SparUser largerUser = manager.getUserMasterById(largerUserId);

        Integer smallerUserPid = smallerUser.getMasterPartitionId();
        Integer largerUserPid  = largerUser.getMasterPartitionId();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPartitionIds().contains(largerUserPid) && largerUser.getReplicaPartitionIds().contains(smallerUserPid);
        if (!colocatedMasters && !colocatedReplicas) {
            BEFRIEND_REBALANCE_STRATEGY strategy = sparBefriendingStrategy.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser);
            performRebalace(strategy, smallerUserId, largerUserId);
        }

        manager.befriend(smallerUser, largerUser);
    }

    void performRebalace(BEFRIEND_REBALANCE_STRATEGY strategy, Integer smallUid, Integer largeUid) {
        SparUser smallerUser = manager.getUserMasterById(smallUid);
        SparUser largerUser = manager.getUserMasterById(largeUid);
        Integer smallerUserPid = smallerUser.getMasterPartitionId();
        Integer largerUserPid = largerUser.getMasterPartitionId();

        if (strategy == NO_CHANGE) {
            if (!smallerUser.getReplicaPartitionIds().contains(largerUserPid)) {
                manager.addReplica(smallerUser, largerUserPid);
            }
            if (!largerUser.getReplicaPartitionIds().contains(smallerUserPid)) {
                manager.addReplica(largerUser, smallerUserPid);
            }
        } else {
            SparUser moving = (strategy == SMALL_TO_LARGE) ? smallerUser : largerUser;
            Integer targetPid = (strategy == SMALL_TO_LARGE) ? largerUserPid : smallerUserPid;
            Set<Integer> replicasToAddInDestinationPartition = sparBefriendingStrategy.findReplicasToAddToTargetPartition(moving, targetPid);
            Set<Integer> replicasToDeleteInSourcePartition = sparBefriendingStrategy.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition);
            manager.moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        //When an edge between smallerUserId and largerUserId disappears,
        //the algorithm removes the replica of smallerUserId in the partition holding the master of node largerUserId
        //if no other node requires it, and vice-versa.
        //The algorithm checks whether there are more than K slave replicas before removing the node so that the desired redundancy level is maintained.

        SparUser smallerUser = manager.getUserMasterById(smallerUserId);
        SparUser largerUser = manager.getUserMasterById(largerUserId);

        if (!smallerUser.getMasterPartitionId().equals(largerUser.getMasterPartitionId())) {
            boolean smallerReplicaWasOnlyThereForLarger = sparBefriendingStrategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(largerUser).contains(smallerUserId);
            boolean largerReplicaWasOnlyThereForSmaller = sparBefriendingStrategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(smallerUser).contains(largerUserId);

            if (smallerReplicaWasOnlyThereForLarger && smallerUser.getReplicaPartitionIds().size() > manager.getMinNumReplicas()) {
                manager.removeReplica(smallerUser, largerUser.getMasterPartitionId());
            }
            if (largerReplicaWasOnlyThereForSmaller && largerUser.getReplicaPartitionIds().size() > manager.getMinNumReplicas()) {
                manager.removeReplica(largerUser, smallerUser.getMasterPartitionId());
            }
        }

        manager.unfriend(smallerUser, largerUser);
    }

    @Override
    public int addPartition() {
        //We use option (2) from the paper:
        //2) let the re-distribution of the masters be the result of the node and edge arrival processes and the load-balancing condition.
        return manager.addPartition();
    }

    @Override
    public void addPartition(Integer partitionId) {
        manager.addPartition(partitionId);
    }

    @Override
    public void removePartition(Integer partitionId) {
        //First, determine which users will be impacted by this action
        Set<Integer> affectedUsers = determineAffectedUsers(partitionId);

        //Second, determine the migration strategy
        Map<Integer, Integer> migrationStrategy = sparMigrationStrategy.getUserMigrationStrategy(partitionId);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keySet()) {
            SparUser user = manager.getUserMasterById(userId);
            Integer newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPartitionIds().contains(newPartitionId)) {
                if(manager.getMinNumReplicas() > 0) {
                    throw new RuntimeException("This should never happen with minNumReplicas > 0!");
                }
                manager.addReplica(user, newPartitionId);
            }
            manager.promoteReplicaToMaster(userId, migrationStrategy.get(userId));
        }

        Set<Integer> usersToReplicate = getUsersToReplicate(affectedUsers, partitionId);

        //Fourth, add replicas as appropriate
        for (Integer userId : usersToReplicate) {
            SparUser user = manager.getUserMasterById(userId);
            int newPid = getRandomPartitionIdWhereThisUserIsNotPresent(user, Arrays.asList(partitionId));
            manager.addReplica(user, newPid);
        }

        //Fifth, remove references to replicas formerly on this partition
        for(Integer uid : manager.getPartitionById(partitionId).getIdsOfReplicas()) {
            SparUser user = manager.getUserMasterById(uid);
            for (Integer currentReplicaPartitionId : user.getReplicaPartitionIds()) {
                manager.getPartitionById(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(partitionId);
            }

            //Delete it from the master's replicaPartitionIds
            user.removeReplicaPartitionId(partitionId);
        }

        //Finally, actually drop partition
        manager.removePartition(partitionId);
    }

    Set<Integer> determineUsersWhoWillNeedAnAdditionalReplica(Integer partitionIdToBeRemoved) {
        SparPartition partition = manager.getPartitionById(partitionIdToBeRemoved);
        Set<Integer> usersInNeedOfNewReplicas = new HashSet<Integer>();

        //First, determine which users will need more replicas once this partition is kaput
        for (Integer userId : partition.getIdsOfMasters()) {
            SparUser user = manager.getUserMasterById(userId);
            if (user.getReplicaPartitionIds().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        for (Integer userId : partition.getIdsOfReplicas()) {
            SparUser user = manager.getUserMasterById(userId);
            if (user.getReplicaPartitionIds().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
    }

    Set<Integer> getUsersToReplicate(Set<Integer> uids, Integer pid) {
        Map<Integer, Integer> numReplicasAndMastersNotOnPartitionToBeRemoved = getCountOfReplicasAndMastersNotOnPartition(uids, pid);
        int minReplicas = manager.getMinNumReplicas();
        Set<Integer> usersToReplicate = new HashSet<Integer>();
        for(Integer uid : uids) {
            if(numReplicasAndMastersNotOnPartitionToBeRemoved.get(uid) <= minReplicas) {
                usersToReplicate.add(uid);
            }
        }
        return usersToReplicate;
    }

    Map<Integer, Integer> getCountOfReplicasAndMastersNotOnPartition(Set<Integer> uids, Integer pid) {
        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
        for(Integer uid : uids) {
            int count = 0;
            SparUser user = manager.getUserMasterById(uid);
            count += user.getMasterPartitionId().equals(pid) ? 0 : 1;
            Set<Integer> replicas = user.getReplicaPartitionIds();
            int numReplicas = replicas.size();
            count += replicas.contains(pid) ? numReplicas - 1 : numReplicas;
            counts.put(uid, count);
        }
        return counts;
    }

    Set<Integer> determineAffectedUsers(Integer partitionIdToBeRemoved) {
        SparPartition partition = manager.getPartitionById(partitionIdToBeRemoved);
        Set<Integer> possibilities = new HashSet<Integer>(partition.getIdsOfMasters());
        possibilities.addAll(partition.getIdsOfReplicas());
        return possibilities;
    }

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SparUser user, Collection<Integer> pidsToExclude) {
        Set<Integer> potentialReplicaLocations = new HashSet<Integer>(manager.getAllPartitionIds());
        potentialReplicaLocations.removeAll(pidsToExclude);
        potentialReplicaLocations.remove(user.getMasterPartitionId());
        potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
        List<Integer> list = new LinkedList<Integer>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    @Override
    public Integer getNumberOfPartitions() {
        return manager.getAllPartitionIds().size();
    }

    @Override
    public Integer getNumberOfUsers() {
        return manager.getNumUsers();
    }

    @Override
    public Collection<Integer> getUserIds() {
        return manager.getAllUserIds();
    }

    @Override
    public Collection<Integer> getPartitionIds() {
        return manager.getAllPartitionIds();
    }

    @Override
    public Integer getEdgeCut() {
        return manager.getEdgeCut();
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        return manager.getPartitionToUserMap();
    }

    @Override
    public Integer getReplicationCount() {
        return manager.getReplicationCount();
    }

    @Override
    public void broadcastDowntime() {
        //SPAR ignores downtime
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        return manager.getFriendships();
    }

    @Override
    public double calcualteAssortivity() {
        return ProbabilityUtils.calculateAssortivityCoefficient(getFriendships());
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToReplicaMap() {
        Map<Integer, Set<Integer>> m = new HashMap<Integer, Set<Integer>>();
        for(int pid : getPartitionIds()) {
            m.put(pid, manager.getPartitionById(pid).getIdsOfReplicas());
        }
        return m;
    }

    @Override
    public String toString() {
        return manager.toString();
    }

    @Override
    public double calculateExpectedQueryDelay() {
        return 0; //Replica systems are strictly-local by design
    }
}
