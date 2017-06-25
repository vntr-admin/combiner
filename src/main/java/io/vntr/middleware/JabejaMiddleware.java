package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.set.TIntSet;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.repartition.JRepartitioner;
import io.vntr.repartition.NoRepResults;

import static io.vntr.utils.TroveUtils.getRandomElement;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class JabejaMiddleware extends AbstractNoRepMiddleware {
    private final float alpha;
    private final float initialT;
    private final float deltaT;
    private final int k;
    private final int numRestarts;
    private boolean incremental = false;

    public JabejaMiddleware(float alpha, float initialT, float deltaT, int k, int numRestarts, NoRepManager manager) {
        super(manager);
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        this.numRestarts = numRestarts;
    }

    public JabejaMiddleware(float alpha, float initialT, float deltaT, int k, int numRestarts, boolean incremental, NoRepManager manager) {
        super(manager);
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        this.numRestarts = numRestarts;
        this.incremental = incremental;
    }

    @Override
    public void removePartition(Integer pid) {
        TIntSet partition = getManager().getPartition(pid);
        getManager().removePartition(pid);
        for(TIntIterator iter = partition.iterator(); iter.hasNext(); ) {
            Integer newPid = getRandomElement(getManager().getPids());
            getManager().moveUser(iter.next(), newPid, true);
        }
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    void repartition() {
        NoRepResults noRepResults = JRepartitioner.repartition(alpha, initialT, deltaT, k, numRestarts, getManager().getPartitionToUsers(), getManager().getFriendships(), incremental);
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
