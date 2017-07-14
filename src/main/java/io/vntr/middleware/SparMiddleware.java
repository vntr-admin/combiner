package io.vntr.middleware;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;
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
    public void befriend(Integer smallerUid, Integer largerUid) {
        RepUser smallerUser = getManager().getUserMaster(smallerUid);
        RepUser largerUser = getManager().getUserMaster(largerUid);

        Integer smallerUserPid = smallerUser.getBasePid();
        Integer largerUserPid  = largerUser.getBasePid();

        boolean colocatedMasters = smallerUserPid.equals(largerUserPid);
        boolean colocatedReplicas = smallerUser.getReplicaPids().contains(largerUserPid) && largerUser.getReplicaPids().contains(smallerUserPid);
        if(!colocatedMasters && !colocatedReplicas) {
            BEFRIEND_REBALANCE_STRATEGY strategy = SBefriender.determineBestBefriendingRebalanceStrategy(smallerUser, largerUser, getManager().getMinNumReplicas(), getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap());
            performRebalance(strategy, smallerUid, largerUid);
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
    TIntIntMap getMigrationStrategy(int pid) {
        return SMigrator.getUserMigrationStrategy(pid, getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap(), true);
    }
}
