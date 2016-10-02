package io.vntr.spaja;

import io.vntr.IMiddleware;
import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;

import java.util.*;

import static io.vntr.spaja.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SpajaMiddleware implements IMiddleware, IMiddlewareAnalyzer {

    private SpajaManager manager;
    private SpajaBefriendingStrategy spajaBefriendingStrategy;
    private SpajaMigrationStrategy spajaMigrationStrategy;
    private SpajaRepartitioner spajaRepartitioner;

    public SpajaMiddleware(SpajaManager manager, SpajaBefriendingStrategy spajaBefriendingStrategy, SpajaMigrationStrategy spajaMigrationStrategy) {
        this.manager = manager;
        this.spajaBefriendingStrategy = spajaBefriendingStrategy;
        this.spajaMigrationStrategy = spajaMigrationStrategy;
    }

    public SpajaMiddleware(SpajaManager manager) {
        this.manager = manager;
        spajaBefriendingStrategy = new SpajaBefriendingStrategy(manager);
        spajaMigrationStrategy = new SpajaMigrationStrategy(manager);
        spajaRepartitioner = new SpajaRepartitioner(manager, spajaBefriendingStrategy);
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
    }

    @Override
    public void removeUser(Long userId) {
        manager.removeUser(userId);
    }

    @Override
    public void befriend(Long smallerUserId, Long largerUserId) {
        SpajaUser smallerUser = manager.getUserMasterById(smallerUserId);
        SpajaUser largerUser = manager.getUserMasterById(largerUserId);
        manager.befriend(smallerUser, largerUser);

        Long smallerUserPid = smallerUser.getMasterPartitionId();
        Long largerUserPid  = largerUser.getMasterPartitionId();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPartitionIds().contains(largerUserPid) && largerUser.getReplicaPartitionIds().contains(smallerUserPid);
        if (!colocatedMasters && !colocatedReplicas) {
            BEFRIEND_REBALANCE_STRATEGY strategy = spajaBefriendingStrategy.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser);
            performRebalace(strategy, smallerUserId, largerUserId);
        }
    }

    void performRebalace(BEFRIEND_REBALANCE_STRATEGY strategy, Long smallUid, Long largeUid) {
        SpajaUser smallerUser = manager.getUserMasterById(smallUid);
        SpajaUser largerUser = manager.getUserMasterById(largeUid);
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
            SpajaUser moving = (strategy == SMALL_TO_LARGE) ? smallerUser : largerUser;
            Long targetPid = (strategy == SMALL_TO_LARGE) ? largerUserPid : smallerUserPid;
            Set<Long> replicasToAddInDestinationPartition = spajaBefriendingStrategy.findReplicasToAddToTargetPartition(moving, targetPid);
            Set<Long> replicasToDeleteInSourcePartition = spajaBefriendingStrategy.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition);
            manager.moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
        }
    }

    @Override
    public void unfriend(Long smallerUserId, Long largerUserId) {
        SpajaUser smallerUser = manager.getUserMasterById(smallerUserId);
        SpajaUser largerUser = manager.getUserMasterById(largerUserId);

        if (!smallerUser.getMasterPartitionId().equals(largerUser.getMasterPartitionId())) {
            boolean smallerReplicaWasOnlyThereForLarger = spajaBefriendingStrategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(largerUser).contains(smallerUserId);
            boolean largerReplicaWasOnlyThereForSmaller = spajaBefriendingStrategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(smallerUser).contains(largerUserId);

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
    public void addPartition() { manager.addPartition(); }

    @Override
    public void removePartition(Long partitionId) {
        //First, determine which users will need more replicas once this partition is kaput
        Set<Long> usersInNeedOfNewReplicas = determineUsersWhoWillNeedAnAdditionalReplica(partitionId);

        //Second, determine the migration strategy
        Map<Long, Long> migrationStrategy = spajaMigrationStrategy.getUserMigrationStrategy(partitionId);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Long userId : migrationStrategy.keySet()) {
            SpajaUser user = manager.getUserMasterById(userId);
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
            SpajaUser user = manager.getUserMasterById(userId);
            manager.addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user));
        }

        //Finally, actually drop partition
        manager.removePartition(partitionId);
    }

    Set<Long> determineUsersWhoWillNeedAnAdditionalReplica(Long partitionIdToBeRemoved) {
        SpajaPartition partition = manager.getPartitionById(partitionIdToBeRemoved);
        Set<Long> usersInNeedOfNewReplicas = new HashSet<Long>();

        //First, determine which users will need more replicas once this partition is kaput
        for (Long userId : partition.getIdsOfMasters()) {
            SpajaUser user = manager.getUserMasterById(userId);
            if (user.getReplicaPartitionIds().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        for (Long userId : partition.getIdsOfReplicas()) {
            SpajaUser user = manager.getUserMasterById(userId);
            if (user.getReplicaPartitionIds().size() <= manager.getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
    }

    Long getRandomPartitionIdWhereThisUserIsNotPresent(SpajaUser user) {
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

    @Override
    public void broadcastDowntime() {
        spajaRepartitioner.repartition();
    }
}
