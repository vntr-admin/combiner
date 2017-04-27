package io.vntr.middleware;

import io.vntr.RepUser;
import io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY;
import io.vntr.befriend.SBefriender;
import io.vntr.migration.SMigrator;
import io.vntr.repartition.RepResults;
import io.vntr.repartition.SparmesRepartitioner;
import io.vntr.manager.RepManager;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.utils.Utils.*;
import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;
import static java.util.Collections.singleton;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesMiddleware extends AbstractRepMiddleware {

    private int minNumReplicas;
    private float gamma;
    private int k;

    public SparmesMiddleware(int minNumReplicas, float gamma, int k, RepManager manager) {
        super(manager);
        this.minNumReplicas = minNumReplicas;
        this.gamma = gamma;
        this.k = k;
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        RepUser smallerUser = getManager().getUserMaster(smallerUserId);
        RepUser largerUser = getManager().getUserMaster(largerUserId);

        Integer smallerUserPid = smallerUser.getBasePid();
        Integer largerUserPid  = largerUser.getBasePid();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPids().contains(largerUserPid) && largerUser.getReplicaPids().contains(smallerUserPid);
        if(!colocatedMasters && !colocatedReplicas){
            BEFRIEND_REBALANCE_STRATEGY strategy = SBefriender.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser, getManager().getMinNumReplicas(), getManager().getFriendships(), getManager().getPartitionToUserMap(), getManager().getPartitionToReplicasMap());
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
            Map<Integer, Integer> uidToPidMap = getUToMasterMap(getManager().getPartitionToUserMap());
            Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(getManager().getPartitionToReplicasMap(), getManager().getUids());

            Set<Integer> replicasToAddInDestinationPartition = SBefriender.findReplicasToAddToTargetPartition(moving, targetPid, uidToPidMap, uidToReplicasMap);
            Set<Integer> replicasToDeleteInSourcePartition = SBefriender.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition, getManager().getMinNumReplicas(), uidToReplicasMap, uidToPidMap, getManager().getFriendships());
            getManager().moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
            getManager().increaseTally(1);
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        RepUser smallerUser = getManager().getUserMaster(smallerUserId);
        RepUser largerUser = getManager().getUserMaster(largerUserId);

        if (!smallerUser.getBasePid().equals(largerUser.getBasePid())) {
            Map<Integer, Integer> uidToPidMap = getUToMasterMap(getManager().getPartitionToUserMap());
            Map<Integer, Set<Integer>> friendships = getManager().getFriendships();
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
        Map<Integer, Integer> migrationStrategy = SMigrator.getUserMigrationStrategy(partitionId, getManager().getFriendships(), getManager().getPartitionToUserMap(), getManager().getPartitionToReplicasMap());

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

        //TODO: ensure this is correct
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

    Set<Integer> determineUsersWhoWillNeedAnAdditionalReplica(Integer partitionIdToBeRemoved) {
        Set<Integer> possibilities = new HashSet<>(getManager().getMastersOnPartition(partitionIdToBeRemoved));
        possibilities.addAll(getManager().getReplicasOnPartition(partitionIdToBeRemoved));

        Set<Integer> usersInNeedOfNewReplicas = new HashSet<>();
        for (Integer userId : possibilities) {
            RepUser user = getManager().getUserMaster(userId);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
    }


    @Override
    public Long getMigrationTally() {
        return getManager().getMigrationTally();
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    public void repartition() {
        RepResults repResults = SparmesRepartitioner.repartition(k, 100, gamma, minNumReplicas, getPartitionToUserMap(), getManager().getPartitionToReplicasMap(), getFriendships());
        getManager().increaseTallyLogical(repResults.getNumLogicalMoves());
        physicallyMigrate(repResults);
    }

    void physicallyMigrate(RepResults repResults) {
        Map<Integer, Integer> currentUidToPidMap = getUToMasterMap(getPartitionToUserMap());
        Map<Integer, Set<Integer>> currentUidToReplicasMap = getUToReplicasMap(getManager().getPartitionToReplicasMap(), getManager().getUids());

        for(int uid : getManager().getUids()) {
            int currentPid = currentUidToPidMap.get(uid);
            int newPid = repResults.getUidToPidMap().get(uid);

            //move master
            if(currentPid != newPid) {
                getManager().moveMasterAndInformReplicas(uid, currentPid, newPid);
                getManager().increaseTally(1);
            }

            //add and remove replicas as specified in the repResults
            Set<Integer> newReplicas = repResults.getUidsToReplicaPids().get(uid);
            Set<Integer> currentReplicas = currentUidToReplicasMap.get(uid);
            if(!currentReplicas.equals(newReplicas)) {
                for(int newReplica : newReplicas) {
                    if(!currentReplicas.contains(newReplica)) {
                        getManager().addReplica(getManager().getUserMaster(uid), newReplica);
                    }
                }
                for(int oldReplica : currentReplicas) {
                    if(!newReplicas.contains(oldReplica)) {
                        getManager().removeReplica(getManager().getUserMaster(uid), oldReplica);
                    }
                }
            }
        }

        //shore up friend replicas (ideally would be unnecessary)
        for(int uid : getManager().getUids()) {
            shoreUpFriendReplicas(uid);
        }

        //ensure k replication
        for(int uid : getManager().getUids()) {
            ensureKReplication(uid);
        }
    }

    void shoreUpFriendReplicas(int uid) {
        RepUser user = getManager().getUserMaster(uid);
        int pid = user.getBasePid();
        Set<Integer> friends = new HashSet<>(user.getFriendIDs());
        friends.removeAll(getManager().getMastersOnPartition(pid));
        friends.removeAll(getManager().getReplicasOnPartition(pid));
        for(int friendId : friends) {
            getManager().addReplica(getManager().getUserMaster(friendId), pid);
        }
    }

    void ensureKReplication(int uid) {
        RepUser user = getManager().getUserMaster(uid);
        Set<Integer> replicaLocations = user.getReplicaPids();
        int deficit = minNumReplicas - replicaLocations.size();
        if(deficit > 0) {
            Set<Integer> newLocations = new HashSet<>(getPartitionIds());
            newLocations.removeAll(replicaLocations);
            newLocations.remove(user.getBasePid());
            for(int rPid : ProbabilityUtils.getKDistinctValuesFromList(deficit, newLocations)) {
                getManager().addReplica(user, rPid);
            }
        }
    }
}
