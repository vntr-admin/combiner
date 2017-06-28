package io.vntr.middleware;

import io.vntr.User;
import io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY;
import io.vntr.befriend.HBefriender;
import io.vntr.manager.NoRepManager;

import static io.vntr.befriend.BEFRIEND_REBALANCE_STRATEGY.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermarMiddleware extends HermesMiddleware {
    private final float gamma;
    private NoRepManager manager;

    public HermarMiddleware(float gamma, int k, int maxIterations, NoRepManager manager) {
        super(gamma, k, maxIterations, manager);
        this.gamma = gamma;
        this.manager = manager;
    }

    @Override
    public void befriend(Integer smallerUid, Integer largerUid) {
        super.befriend(smallerUid, largerUid);
        handleRebalancing(smallerUid, largerUid);
    }

    void handleRebalancing(Integer smallerUserId, Integer largerUserId) {
        User smallerUser = manager.getUser(smallerUserId);
        User largerUser  = manager.getUser(largerUserId);

        BEFRIEND_REBALANCE_STRATEGY strategy = HBefriender.determineBestBefriendingRebalanceStrategy(smallerUser.getId(), largerUser.getId(), gamma, manager.getFriendships(), manager.getPartitionToUsers());

        if(strategy == SMALL_TO_LARGE) {
            manager.moveUser(smallerUserId, largerUser.getBasePid(), false);
        }
        else if(strategy == LARGE_TO_SMALL) {
            manager.moveUser(largerUserId, smallerUser.getBasePid(), false);
        }
    }

}
