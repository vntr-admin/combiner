package io.vntr.middleware;

import gnu.trove.map.TIntIntMap;
import io.vntr.befriend.JBefriender;
import io.vntr.manager.NoRepManager;
import io.vntr.migration.HMigrator;

import java.util.Map;

import static io.vntr.utils.TroveUtils.convert;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class J2Middleware extends JabejaMiddleware {
    private final float alpha;
    private final int k;

    public J2Middleware(float alpha, float initialT, float deltaT, int k, NoRepManager manager) {
        super(alpha, initialT, deltaT, k, 1, true, manager);
        this.alpha = alpha;
        this.k = k;
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        super.befriend(smallerUserId, largerUserId);
        rebalance(smallerUserId, largerUserId);
    }

    void rebalance(Integer smallerUserId, Integer largerUserId) {
        JBefriender.Result result = JBefriender.rebalance(smallerUserId, largerUserId, k, alpha, convert(getFriendships()), convert(getPartitionToUserMap()));
        Integer uid1 = result.getUid1();
        Integer uid2 = result.getUid2();
        if(uid1 != null && uid2 != null) {
            int pid1 = getManager().getPidForUser(uid1);
            int pid2 = getManager().getPidForUser(uid2);
            getManager().moveUser(uid1, pid2, false);
            getManager().moveUser(uid2, pid1, false);
        }
    }

    @Override
    public void removePartition(Integer partitionId) {
        TIntIntMap targets = HMigrator.migrateOffPartition(partitionId, 1f, convert(getPartitionToUserMap()), convert(getFriendships()));
        for(Integer uid : targets.keys()) {
            getManager().moveUser(uid, targets.get(uid), true);
        }
        getManager().removePartition(partitionId);
    }
}
