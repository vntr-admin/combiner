package io.vntr.middleware;

import gnu.trove.map.TIntIntMap;
import io.vntr.befriend.JBefriender;
import io.vntr.manager.NoRepManager;
import io.vntr.migration.HMigrator;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class J2Middleware extends JabarMiddleware {

    public J2Middleware(float alpha, float initialT, float deltaT, int k, NoRepManager manager) {
        super(alpha, initialT, deltaT, k, manager);
    }

    @Override
    public void removePartition(Integer partitionId) {
        TIntIntMap targets = HMigrator.migrateOffPartition(partitionId, 1f, getManager().getPartitionToUsers(), getManager().getFriendships());
        for(Integer uid : targets.keys()) {
            getManager().moveUser(uid, targets.get(uid), true);
        }
        getManager().removePartition(partitionId);
    }
}
