package io.vntr.spar;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.vntr.IMiddleware;
import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;

import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;

public class SparMiddleware implements IMiddleware, IMiddlewareAnalyzer {
    private SparManager manager;
    private SparBefriendingStrategy sparBefriendingStrategy;
    private SparMigrationStrategy sparMigrationStrategy;

    public SparMiddleware(int minNumReplicas) {
        manager = new SparManager(minNumReplicas);
        sparBefriendingStrategy = new SparBefriendingStrategy(manager);
        sparMigrationStrategy = new SparMigrationStrategy(manager);
    }

    SparMiddleware(SparManager manager) {
        this.manager = manager;
        sparBefriendingStrategy = new SparBefriendingStrategy(manager);
        sparMigrationStrategy = new SparMigrationStrategy(manager);
    }

    public void addUser(User user) {
        manager.addUser(user);
    }

    public void removeUser(Long userId) {
        manager.removeUser(userId);
    }

    public void befriend(Long smallerUserId, Long largerUserId) {
        SparUser smallerUser = manager.getUserMasterById(smallerUserId);
        SparUser largerUser = manager.getUserMasterById(largerUserId);
        manager.befriend(smallerUser, largerUser);

        Long smallerUserPid = smallerUser.getMasterPartitionId();
        Long largerUserPid  = largerUser.getMasterPartitionId();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPartitionIds().contains(largerUserPid) && largerUser.getReplicaPartitionIds().contains(smallerUserPid);
        if (!colocatedMasters && !colocatedReplicas) {
            BEFRIEND_REBALANCE_STRATEGY strategy = sparBefriendingStrategy.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser);
            performRebalace(strategy, smallerUserId, largerUserId);
        }
    }

    void performRebalace(BEFRIEND_REBALANCE_STRATEGY strategy, Long smallUid, Long largeUid) {
        SparUser smallerUser = manager.getUserMasterById(smallUid);
        SparUser largerUser = manager.getUserMasterById(largeUid);
        Long smallerUserPid = smallerUser.getMasterPartitionId();
        Long largerUserPid = largerUser.getMasterPartitionId();

        if (strategy == NO_CHANGE) {
            if (!smallerUser.getReplicaPartitionIds().contains(largerUserPid)) {
                manager.addReplica(smallerUser, largerUserPid);
            }
            if (!largerUser.getReplicaPartitionIds().contains(smallerUserPid)) {
                manager.addReplica(largerUser, smallerUserPid);
            }
        } else {
            SparUser moving = (strategy == SMALL_TO_LARGE) ? smallerUser : largerUser;
            Long targetPid = (strategy == SMALL_TO_LARGE) ? largerUserPid : smallerUserPid;
            Set<Long> replicasToAddInDestinationPartition = sparBefriendingStrategy.findReplicasToAddToTargetPartition(moving, targetPid);
            Set<Long> replicasToDeleteInSourcePartition = sparBefriendingStrategy.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition);
            manager.moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
        }
    }

    public void unfriend(Long smallerUserId, Long largerUserId) {
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

    public void addPartition() {
        //We use option (2) from the paper:
        //2) let the re-distribution of the masters be the result of the node and edge arrival processes and the load-balancing condition.
        manager.addPartition();
    }

    public void removePartition(Long partitionId) {
        //First, determine which users will need more replicas once this partition is kaput
        Set<Long> usersInNeedOfNewReplicas = determineUsersWhoWillNeedAnAdditionalReplica(partitionId);

        //Second, determine the migration strategy
        Map<Long, Long> migrationStrategy = sparMigrationStrategy.getUserMigrationStrategy(partitionId);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Long userId : migrationStrategy.keySet()) {
            SparUser user = manager.getUserMasterById(userId);
            Long newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPartitionIds().contains(newPartitionId)) {
                manager.addReplica(user, newPartitionId);
                usersInNeedOfNewReplicas.remove(userId);
            }
            manager.promoteReplicaToMaster(userId, migrationStrategy.get(userId));
        }

        //Fourth, add replicas as appropriate
        for (Long userId : usersInNeedOfNewReplicas) {
            SparUser user = manager.getUserMasterById(userId);
            manager.addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user));
        }

        //Finally, actually drop partition
        manager.removePartition(partitionId);
    }

    Set<Long> determineUsersWhoWillNeedAnAdditionalReplica(Long partitionIdToBeRemoved) {
        SparPartition partition = manager.getPartitionById(partitionIdToBeRemoved);
        Set<Long> usersInNeedOfNewReplicas = new HashSet<Long>();

        //First, determine which users will need more replicas once this partition is kaput
        for (Long userId : partition.getIdsOfMasters()) {
            SparUser user = manager.getUserMasterById(userId);
            if (user.getReplicaPartitionIds().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        for (Long userId : partition.getIdsOfReplicas()) {
            SparUser user = manager.getUserMasterById(userId);
            if (user.getReplicaPartitionIds().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
    }

    Long getRandomPartitionIdWhereThisUserIsNotPresent(SparUser user) {
        Set<Long> potentialReplicaLocations = new HashSet<Long>(manager.getAllPartitionIds());
        potentialReplicaLocations.remove(user.getMasterPartitionId());
        potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
        List<Long> list = new LinkedList<Long>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    @Override
    public Long getNumberOfPartitions() {
        return (long) manager.getAllPartitionIds().size();
    }

    @Override
    public Long getNumberOfUsers() {
        return (long) manager.getNumUsers();
    }

    @Override
    public Long getEdgeCut() {
        return manager.getEdgeCut();
    }

    @Override
    public Map<Long, Set<Long>> getPartitionToUserMap() {
        return manager.getPartitionToUserMap();
    }

    @Override
    public Long getReplicationCount() {
        return manager.getReplicationCount();
    }
}
