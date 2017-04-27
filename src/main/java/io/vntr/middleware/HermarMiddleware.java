package io.vntr.middleware;

import io.vntr.User;
import io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY;
import io.vntr.befriend.HBefriender;
import io.vntr.manager.NoRepManager;
import io.vntr.migration.HMigrator;
import io.vntr.repartition.HRepartitioner;
import io.vntr.repartition.NoRepResults;

import java.util.*;

import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermarMiddleware extends AbstractNoRepMiddleware {
    private final float gamma;
    private final int k;
    private final int maxIterations;
    private NoRepManager manager;

    public HermarMiddleware(float gamma, int k, int maxIterations, NoRepManager manager) {
        super(manager);
        this.gamma = gamma;
        this.k = k;
        this.maxIterations = maxIterations;
        this.manager = manager;
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        super.befriend(smallerUserId, largerUserId);
        handleRebalancing(smallerUserId, largerUserId);
    }

    void handleRebalancing(Integer smallerUserId, Integer largerUserId) {
        User smallerUser = manager.getUser(smallerUserId);
        User largerUser  = manager.getUser(largerUserId);

        BEFRIEND_REBALANCE_STRATEGY strategy = HBefriender.determineBestBefriendingRebalanceStrategy(smallerUser.getId(), largerUser.getId(), getGamma(), getFriendships(), getPartitionToUserMap());

        if(strategy == SMALL_TO_LARGE) {
            manager.moveUser(smallerUserId, largerUser.getBasePid(), false);
        }
        else if(strategy == LARGE_TO_SMALL) {
            manager.moveUser(largerUserId, smallerUser.getBasePid(), false);
        }
    }

    @Override
    public void removePartition(Integer partitionId) {
        Map<Integer, Integer> targets = HMigrator.migrateOffPartition(partitionId, gamma, manager.getPartitionToUsers(), manager.getFriendships());
        for(Integer uid : targets.keySet()) {
            manager.moveUser(uid, targets.get(uid), true);
        }
        manager.removePartition(partitionId);
        repartition();
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    public float getGamma() {
        return gamma;
    }

    void repartition() {
        NoRepResults noRepResults = HRepartitioner.repartition(k, maxIterations, gamma, manager.getPartitionToUsers(), getFriendships());
        int numMoves = noRepResults.getLogicalMoves();
        if(numMoves > 0) {
            manager.increaseTallyLogical(numMoves);
            physicallyMigrate(noRepResults.getUidsToPids());
        }
    }

    void physicallyMigrate(Map<Integer, Integer> uidsToPids) {
        for(int uid : uidsToPids.keySet()) {
            int pid = uidsToPids.get(uid);
            User user = manager.getUser(uid);
            if(user.getBasePid() != pid) {
                manager.moveUser(uid, pid, false);
            }
        }
    }
}
