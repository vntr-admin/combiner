package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY;
import io.vntr.befriend.SBefriender;
import io.vntr.manager.RepManager;
import io.vntr.migration.SMigrator;

import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;
import static io.vntr.utils.TroveUtils.*;

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
            performRebalance(strategy, smallerUserId, largerUserId);
        }

        getManager().befriend(smallerUser, largerUser);
    }

    void performRebalance(BEFRIEND_REBALANCE_STRATEGY strategy, Integer smallUid, Integer largeUid) {
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
            TIntIntMap uidToPidMap = getUToMasterMap(getPartitionToUserMap());
            TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(getPartitionToReplicasMap(), getUserIds());

            TIntSet replicasToAddInDestinationPartition = SBefriender.findReplicasToAddToTargetPartition(moving, targetPid, uidToPidMap, uidToReplicasMap);
            TIntSet replicasToDeleteInSourcePartition = SBefriender.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition, getManager().getMinNumReplicas(), uidToReplicasMap, uidToPidMap, getFriendships());
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
            TIntIntMap uidToPidMap = getUToMasterMap(getPartitionToUserMap());
            TIntObjectMap<TIntSet> friendships = getFriendships();
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
        TIntSet affectedUsers = determineAffectedUsers(partitionId);

        //Second, determine the migration strategy
        TIntIntMap migrationStrategy = SMigrator.getUserMigrationStrategy(partitionId, getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap(), true);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keys()) {
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

        TIntSet usersToReplicate = getUsersToReplicate(affectedUsers, partitionId);

        //Fourth, add replicas as appropriate
        for(TIntIterator iter = usersToReplicate.iterator(); iter.hasNext(); ) {
            RepUser user = getManager().getUserMaster(iter.next());
            getManager().addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user, singleton(partitionId)));
        }

        //Fifth, remove references to replicas formerly on this partition
        for(TIntIterator iter = getManager().getReplicasOnPartition(partitionId).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            for(TIntIterator iter2 = user.getReplicaPids().iterator(); iter2.hasNext(); ) {
                getManager().getReplicaOnPartition(user.getId(), iter2.next()).removeReplicaPartitionId(partitionId);
            }

            //Delete it from the master's replicaPartitionIds
            user.removeReplicaPartitionId(partitionId);
        }

        //Finally, actually drop partition
        getManager().removePartition(partitionId);
    }

    TIntSet getUsersToReplicate(TIntSet uids, Integer pid) {
        TIntIntMap numReplicasAndMastersNotOnPartitionToBeRemoved = getCountOfReplicasAndMastersNotOnPartition(uids, pid);
        int minReplicas = getManager().getMinNumReplicas();
        TIntSet usersToReplicate = new TIntHashSet();
        for(TIntIterator iter = uids.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            if(numReplicasAndMastersNotOnPartitionToBeRemoved.get(uid) <= minReplicas) {
                usersToReplicate.add(uid);
            }
        }
        return usersToReplicate;
    }

    TIntIntMap getCountOfReplicasAndMastersNotOnPartition(TIntSet uids, Integer pid) {
        TIntIntMap counts = new TIntIntHashMap();
        for(TIntIterator iter = uids.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            int count = 0;
            RepUser user = getManager().getUserMaster(uid);
            count += user.getBasePid().equals(pid) ? 0 : 1;
            TIntSet replicas = user.getReplicaPids();
            int numReplicas = replicas.size();
            count += replicas.contains(pid) ? numReplicas - 1 : numReplicas;
            counts.put(uid, count);
        }
        return counts;
    }

    TIntSet determineAffectedUsers(Integer partitionIdToBeRemoved) {
        TIntSet possibilities = new TIntHashSet(getManager().getMastersOnPartition(partitionIdToBeRemoved));
        possibilities.addAll(getManager().getReplicasOnPartition(partitionIdToBeRemoved));
        return possibilities;
    }

}
