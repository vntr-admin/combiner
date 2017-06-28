package io.vntr.middleware;

import io.vntr.befriend.JBefriender;
import io.vntr.manager.NoRepManager;

/**
 * Created by robertlindquist on 6/27/17.
 */
public class JarmesMiddleware extends HejarMiddleware {
    private int jaK;
    private float alpha;

    public JarmesMiddleware(float gamma, int k, int maxIterations, int jaK, float alpha, float initialT, float deltaT, NoRepManager manager) {
        super(gamma, k, maxIterations, jaK, alpha, initialT, deltaT, manager);
        this.jaK = jaK;
        this.alpha = alpha;
    }

    @Override
    public void befriend(Integer smallerUid, Integer largerUid) {
        super.befriend(smallerUid, largerUid);
        rebalance(smallerUid, largerUid);
    }

    void rebalance(Integer smallerUserId, Integer largerUserId) {
        JBefriender.Result result = JBefriender.rebalance(smallerUserId, largerUserId, jaK, alpha, getManager().getFriendships(), getManager().getPartitionToUsers());
        Integer uid1 = result.getUid1();
        Integer uid2 = result.getUid2();
        if(uid1 != null && uid2 != null) {
            int pid1 = getManager().getPidForUser(uid1);
            int pid2 = getManager().getPidForUser(uid2);
            getManager().moveUser(uid1, pid2, false);
            getManager().moveUser(uid2, pid1, false);
        }
    }


}
