package io.vntr.middleware;

import io.vntr.befriend.JBefriender;
import io.vntr.manager.NoRepManager;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class JabarMiddleware extends JabejaMiddleware {
    private final float alpha;
    private final short k;

    public JabarMiddleware(float alpha, float initialT, float deltaT, short k, NoRepManager manager) {
        super(alpha, initialT, deltaT, k, 1, true, manager);
        this.alpha = alpha;
        this.k = k;
    }

    @Override
    public void befriend(short smallerUid, short largerUid) {
        super.befriend(smallerUid, largerUid);
        rebalance(smallerUid, largerUid);
    }

    void rebalance(short smallerUserId, short largerUserId) {
        JBefriender.Result result = JBefriender.rebalance(smallerUserId, largerUserId, k, alpha, getManager().getFriendships(), getManager().getPartitionToUsers());
        Short uid1 = result.getUid1();
        Short uid2 = result.getUid2();
        if(uid1 != null && uid2 != null) {
            short pid1 = getManager().getPidForUser(uid1);
            short pid2 = getManager().getPidForUser(uid2);
            getManager().moveUser(uid1, pid2, false);
            getManager().moveUser(uid2, pid1, false);
        }
    }

}
