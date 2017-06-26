package io.vntr.middleware;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
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
    public void befriend(short smallerUid, short largerUid) {
        RepUser smallerUser = getManager().getUserMaster(smallerUid);
        RepUser largerUser = getManager().getUserMaster(largerUid);

        short smallerUserPid = smallerUser.getBasePid();
        short largerUserPid  = largerUser.getBasePid();

        boolean colocatedMasters = smallerUserPid == largerUserPid;
        boolean colocatedReplicas = smallerUser.getReplicaPids().contains(largerUserPid) && largerUser.getReplicaPids().contains(smallerUserPid);
        if(!colocatedMasters && !colocatedReplicas) {
            BEFRIEND_REBALANCE_STRATEGY strategy = SBefriender.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser, getManager().getMinNumReplicas(), getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap());
            performRebalance(strategy, smallerUid, largerUid);
        }

        getManager().befriend(smallerUser, largerUser);
    }

    void performRebalance(BEFRIEND_REBALANCE_STRATEGY strategy, short smallUid, short largeUid) {
        RepUser smallerUser = getManager().getUserMaster(smallUid);
        RepUser largerUser = getManager().getUserMaster(largeUid);
        short smallerUserPid = smallerUser.getBasePid();
        short largerUserPid = largerUser.getBasePid();

        if (strategy == NO_CHANGE) {
            if (!smallerUser.getReplicaPids().contains(largerUserPid)) {
                getManager().addReplica(smallerUser, largerUserPid);
            }
            if (!largerUser.getReplicaPids().contains(smallerUserPid)) {
                getManager().addReplica(largerUser, smallerUserPid);
            }
        } else {
            RepUser moving = (strategy == SMALL_TO_LARGE) ? smallerUser : largerUser;
            short targetPid = (strategy == SMALL_TO_LARGE) ? largerUserPid : smallerUserPid;
            TShortShortMap uidToPidMap = getUToMasterMap(getPartitionToUserMap());
            TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(getPartitionToReplicasMap(), getUserIds());

            TShortSet replicasToAddInDestinationPartition = SBefriender.findReplicasToAddToTargetPartition(moving, targetPid, uidToPidMap, uidToReplicasMap);
            TShortSet replicasToDeleteInSourcePartition = SBefriender.findReplicasInMovingPartitionToDelete(moving, replicasToAddInDestinationPartition, getManager().getMinNumReplicas(), uidToReplicasMap, uidToPidMap, getFriendships());
            getManager().moveUser(moving, targetPid, replicasToAddInDestinationPartition, replicasToDeleteInSourcePartition);
        }
    }

    @Override
    public void unfriend(short smallerUid, short largerUid) {
        //When an edge between smallerUserId and largerUserId disappears,
        //the algorithm removes the replica of smallerUserId in the partition holding the master of node largerUserId
        //if no other node requires it, and vice-versa.
        //The algorithm checks whether there are more than K slave replicas before removing the node so that the desired redundancy level is maintained.

        RepUser smallerUser = getManager().getUserMaster(smallerUid);
        RepUser largerUser = getManager().getUserMaster(largerUid);

        if (smallerUser.getBasePid() != largerUser.getBasePid()) {
            TShortShortMap uidToPidMap = getUToMasterMap(getPartitionToUserMap());
            TShortObjectMap<TShortSet> friendships = getFriendships();
            boolean smallerReplicaWasOnlyThereForLarger = SBefriender.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(largerUser, uidToPidMap, friendships).contains(smallerUid);
            boolean largerReplicaWasOnlyThereForSmaller = SBefriender.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(smallerUser, uidToPidMap, friendships).contains(largerUid);

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
    public void removePartition(short pid) {
        //First, determine which users will be impacted by this action
        TShortSet affectedUsers = determineAffectedUsers(pid);

        //Second, determine the migration strategy
        TShortShortMap migrationStrategy = SMigrator.getUserMigrationStrategy(pid, getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap(), true);

        //Third, promote replicas to masters as specified in the migration strategy
        for (short uid : migrationStrategy.keys()) {
            RepUser user = getManager().getUserMaster(uid);
            short newPid = migrationStrategy.get(uid);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPid)) {
                if(getManager().getMinNumReplicas() > 0) {
                    throw new RuntimeException("This should never happen with minNumReplicas > 0!");
                }
                getManager().addReplica(user, newPid);
            }
            getManager().promoteReplicaToMaster(uid, newPid);
        }

        TShortSet usersToReplicate = getUsersToReplicate(affectedUsers, pid);

        //Fourth, add replicas as appropriate
        for(TShortIterator iter = usersToReplicate.iterator(); iter.hasNext(); ) {
            RepUser user = getManager().getUserMaster(iter.next());
            getManager().addReplica(user, getRandomPidWhereThisUserIsNotPresent(user, singleton(pid)));
        }

        //Fifth, remove references to replicas formerly on this partition
        for(TShortIterator iter = getManager().getReplicasOnPartition(pid).iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            for(TShortIterator iter2 = user.getReplicaPids().iterator(); iter2.hasNext(); ) {
                getManager().getReplicaOnPartition(user.getId(), iter2.next()).removeReplicaPid(pid);
            }

            //Delete it from the master's replicaPids
            user.removeReplicaPid(pid);
        }

        //Finally, actually drop partition
        getManager().removePartition(pid);
    }

    TShortSet getUsersToReplicate(TShortSet uids, short pid) {
        TShortShortMap numReplicasAndMastersNotOnPartitionToBeRemoved = getCountOfReplicasAndMastersNotOnPartition(uids, pid);
        int minReplicas = getManager().getMinNumReplicas();
        TShortSet usersToReplicate = new TShortHashSet();
        for(TShortIterator iter = uids.iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            if(numReplicasAndMastersNotOnPartitionToBeRemoved.get(uid) <= minReplicas) {
                usersToReplicate.add(uid);
            }
        }
        return usersToReplicate;
    }

    TShortShortMap getCountOfReplicasAndMastersNotOnPartition(TShortSet uids, short pid) {
        TShortShortMap counts = new TShortShortHashMap(uids.size()+1);
        for(TShortIterator iter = uids.iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            short count = 0;
            RepUser user = getManager().getUserMaster(uid);
            count += user.getBasePid() == (pid) ? 0 : 1;
            TShortSet replicas = user.getReplicaPids();
            int numReplicas = replicas.size();
            count += replicas.contains(pid) ? numReplicas - 1 : numReplicas;
            counts.put(uid, count);
        }
        return counts;
    }

    TShortSet determineAffectedUsers(short pidToBeRemoved) {
        TShortSet possibilities = new TShortHashSet(getManager().getMastersOnPartition(pidToBeRemoved));
        possibilities.addAll(getManager().getReplicasOnPartition(pidToBeRemoved));
        return possibilities;
    }

}
