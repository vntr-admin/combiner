package io.vntr.middleware;

import gnu.trove.map.TShortShortMap;
import io.vntr.User;
import io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY;
import io.vntr.befriend.HBefriender;
import io.vntr.manager.NoRepManager;
import io.vntr.migration.HMigrator;
import io.vntr.repartition.HRepartitioner;
import io.vntr.repartition.NoRepResults;

import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermarMiddleware extends AbstractNoRepMiddleware {
    private final float gamma;
    private final short k;
    private final short maxIterations;
    private NoRepManager manager;

    public HermarMiddleware(float gamma, short k, short maxIterations, NoRepManager manager) {
        super(manager);
        this.gamma = gamma;
        this.k = k;
        this.maxIterations = maxIterations;
        this.manager = manager;
    }

    @Override
    public void befriend(short smallerUid, short largerUid) {
        super.befriend(smallerUid, largerUid);
        handleRebalancing(smallerUid, largerUid);
    }

    void handleRebalancing(short smallerUserId, short largerUserId) {
        User smallerUser = manager.getUser(smallerUserId);
        User largerUser  = manager.getUser(largerUserId);

        BEFRIEND_REBALANCE_STRATEGY strategy = HBefriender.determineBestBefriendingRebalanceStrategy(smallerUser.getId(), largerUser.getId(), getGamma(), manager.getFriendships(), manager.getPartitionToUsers());

        if(strategy == SMALL_TO_LARGE) {
            manager.moveUser(smallerUserId, largerUser.getBasePid(), false);
        }
        else if(strategy == LARGE_TO_SMALL) {
            manager.moveUser(largerUserId, smallerUser.getBasePid(), false);
        }
    }

    @Override
    public void removePartition(short pid) {
        TShortShortMap targets = HMigrator.migrateOffPartition(pid, gamma, manager.getPartitionToUsers(), manager.getFriendships());
        for(short uid : targets.keys()) {
            manager.moveUser(uid, targets.get(uid), true);
        }
        manager.removePartition(pid);
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
        NoRepResults noRepResults = HRepartitioner.repartition(k, maxIterations, gamma, manager.getPartitionToUsers(), manager.getFriendships());
        int numMoves = noRepResults.getLogicalMoves();
        if(numMoves > 0) {
            manager.increaseTallyLogical(numMoves);
            physicallyMigrate(noRepResults.getUidsToPids());
        }
    }

    void physicallyMigrate(TShortShortMap uidsToPids) {
        for(short uid : uidsToPids.keys()) {
            short pid = uidsToPids.get(uid);
            User user = manager.getUser(uid);
            if(user.getBasePid() != pid) {
                manager.moveUser(uid, pid, false);
            }
        }
    }
}
