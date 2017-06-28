package io.vntr.middleware;

import gnu.trove.map.TIntIntMap;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.repartition.JRepartitioner;
import io.vntr.repartition.NoRepResults;

/**
 * Created by robertlindquist on 6/27/17.
 */
public class HejarMiddleware extends H2Middleware {
    private static final int numRestarts = 1;
    private static final boolean incremental = true;
    private int jaK;
    private float alpha;
    private float initialT;
    private float deltaT;

    public HejarMiddleware(float gamma, int k, int maxIterations, int jaK, float alpha, float initialT, float deltaT, NoRepManager manager) {
        super(gamma, k, maxIterations, manager);
        this.jaK = jaK;
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
    }

    @Override
    public void broadcastDowntime() {
        jRepartition();
    }

    void jRepartition() {
        NoRepResults noRepResults = JRepartitioner.repartition(alpha, initialT, deltaT, jaK, numRestarts, getManager().getPartitionToUsers(), getManager().getFriendships(), incremental, false);
        getManager().increaseTallyLogical(noRepResults.getLogicalMoves());
        if(noRepResults.getUidsToPids() != null) {
            physicallyMigrate(noRepResults.getUidsToPids());
        }
    }

    void physicallyMigrate(TIntIntMap logicalPids) {
        for(Integer uid : logicalPids.keys()) {
            User user = getManager().getUser(uid);
            Integer newPid = logicalPids.get(uid);
            if(!user.getBasePid().equals(newPid)) {
                getManager().moveUser(uid, newPid, false);
            }
        }
    }
}
