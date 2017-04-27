package io.vntr.middleware;

import java.util.*;

import io.vntr.RepUser;
import io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY;
import io.vntr.befriend.SBefriender;
import io.vntr.manager.RepManager;
import io.vntr.migration.SMigrator;

import static io.vntr.utils.Utils.*;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;
import static java.util.Collections.singleton;

public class SparMiddleware extends AbstractRepMiddleware {

    public SparMiddleware(RepManager manager) {
        super(manager);
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        RepUser smallerUser = getManager().getUserMaster(smallerUserId);
        RepUser largerUser = getManager().getUserMaster(largerUserId);

        Integer smallerUserPid = smallerUser.getBasePid();
        Integer largerUserPid  = largerUser.getBasePid();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPids().contains(largerUserPid) && largerUser.getReplicaPids().contains(smallerUserPid);
        if(!colocatedMasters && !colocatedReplicas) {
            BEFRIEND_REBALANCE_STRATEGY strategy = SBefriender.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser, getManager().getMinNumReplicas(), getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap());
            performRebalace(strategy, smallerUserId, largerUserId);
        }

        getManager().befriend(smallerUser, largerUser);
    }

    void performRebalace(BEFRIEND_REBALANCE_STRATEGY strategy, Integer smallUid, Integer largeUid) {
        RepUser smallerUser = getManager().getUserMaster(smallUid);
        RepUser largerUser = getManager().getUserMaster(largeUid);
        Integer smallerUserPid = smallerUser.getBasePid();
        Integer largerUserPid = largerUser.getBasePid();

        if (strategy == NO_CHANGE) {
            if (!smallerUser.getReplicaPids().contains(largerUserPid)) {
                getManager().addReplica(smallerUser, largerUserPid);
            }
            if (!largerUser.getReplicaPids().contains(smallerUserPid)) {
                getManager().addReplica(largerUser, smallerUserPid);
            }
        } else {
            RepUser moving = (strategy == SMALL_TO_LARGE) ? smallerUser : largerUser;
            Integer targetPid = (strategy == SMALL_TO_LARGE) ? largerUserPid : smallerUserPid;
            Map<Integer, Integer> uidToPidMap = getUToMasterMap(getPartitionToUserMap());
            Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(getPartitionToReplicasMap(), getUserIds());

            Set<Integer> replicasToAddInDestinationPartition = SBefriender.findReplicasToAddToTargetPartition(moving, targetPid, uidToPidMap, uidToReplicasMap);
            Set<Integer> replicasToDeleteInSourcePartition = SBefriender.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition, getManager().getMinNumReplicas(), uidToReplicasMap, uidToPidMap, getFriendships());
            getManager().moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        //When an edge between smallerUserId and largerUserId disappears,
        //the algorithm removes the replica of smallerUserId in the partition holding the master of node largerUserId
        //if no other node requires it, and vice-versa.
        //The algorithm checks whether there are more than K slave replicas before removing the node so that the desired redundancy level is maintained.

        RepUser smallerUser = getManager().getUserMaster(smallerUserId);
        RepUser largerUser = getManager().getUserMaster(largerUserId);

        if (!smallerUser.getBasePid().equals(largerUser.getBasePid())) {
            Map<Integer, Integer> uidToPidMap = getUToMasterMap(getPartitionToUserMap());
            Map<Integer, Set<Integer>> friendships = getFriendships();
            boolean smallerReplicaWasOnlyThereForLarger = SBefriender.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(largerUser, uidToPidMap, friendships).contains(smallerUserId);
            boolean largerReplicaWasOnlyThereForSmaller = SBefriender.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(smallerUser, uidToPidMap, friendships).contains(largerUserId);

            if (smallerReplicaWasOnlyThereForLarger && smallerUser.getReplicaPids().size() > getManager().getMinNumReplicas()) {
                getManager().removeReplica(smallerUser, largerUser.getBasePid());
            }
            if (largerReplicaWasOnlyThereForSmaller && largerUser.getReplicaPids().size() > getManager().getMinNumReplicas()) {
                getManager().removeReplica(largerUser, smallerUser.getBasePid());
            }
        }

        getManager().unfriend(smallerUser, largerUser);
    }

    @Override
    public void removePartition(Integer partitionId) {
        //First, determine which users will be impacted by this action
        Set<Integer> affectedUsers = determineAffectedUsers(partitionId);

        //Second, determine the migration strategy
        Map<Integer, Integer> migrationStrategy = SMigrator.getUserMigrationStrategy(partitionId, getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap());

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keySet()) {
            RepUser user = getManager().getUserMaster(userId);
            Integer newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPartitionId)) {
                if(getManager().getMinNumReplicas() > 0) {
                    throw new RuntimeException("This should never happen with minNumReplicas > 0!");
                }
                getManager().addReplica(user, newPartitionId);
            }
            getManager().promoteReplicaToMaster(userId, newPartitionId);
        }

        Set<Integer> usersToReplicate = getUsersToReplicate(affectedUsers, partitionId);

        //Fourth, add replicas as appropriate
        for (Integer userId : usersToReplicate) {
            RepUser user = getManager().getUserMaster(userId);
            getManager().addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user, singleton(partitionId)));
        }

        //Fifth, remove references to replicas formerly on this partition
        for(Integer uid : getManager().getReplicasOnPartition(partitionId)) {
            RepUser user = getManager().getUserMaster(uid);
            for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
                getManager().getReplicaOnPartition(user.getId(), currentReplicaPartitionId).removeReplicaPartitionId(partitionId);
            }

            //Delete it from the master's replicaPartitionIds
            user.removeReplicaPartitionId(partitionId);
        }

        //Finally, actually drop partition
        getManager().removePartition(partitionId);
    }

    Set<Integer> getUsersToReplicate(Set<Integer> uids, Integer pid) {
        Map<Integer, Integer> numReplicasAndMastersNotOnPartitionToBeRemoved = getCountOfReplicasAndMastersNotOnPartition(uids, pid);
        int minReplicas = getManager().getMinNumReplicas();
        Set<Integer> usersToReplicate = new HashSet<>();
        for(Integer uid : uids) {
            if(numReplicasAndMastersNotOnPartitionToBeRemoved.get(uid) <= minReplicas) {
                usersToReplicate.add(uid);
            }
        }
        return usersToReplicate;
    }

    Map<Integer, Integer> getCountOfReplicasAndMastersNotOnPartition(Set<Integer> uids, Integer pid) {
        Map<Integer, Integer> counts = new HashMap<>();
        for(Integer uid : uids) {
            int count = 0;
            RepUser user = getManager().getUserMaster(uid);
            count += user.getBasePid().equals(pid) ? 0 : 1;
            Set<Integer> replicas = user.getReplicaPids();
            int numReplicas = replicas.size();
            count += replicas.contains(pid) ? numReplicas - 1 : numReplicas;
            counts.put(uid, count);
        }
        return counts;
    }

    Set<Integer> determineAffectedUsers(Integer partitionIdToBeRemoved) {
        Set<Integer> possibilities = new HashSet<>(getManager().getMastersOnPartition(partitionIdToBeRemoved));
        possibilities.addAll(getManager().getReplicasOnPartition(partitionIdToBeRemoved));
        return possibilities;
    }

}
