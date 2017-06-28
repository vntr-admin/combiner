package io.vntr.middleware;

import gnu.trove.map.TIntIntMap;
import io.vntr.manager.NoRepManager;
import io.vntr.migration.HMigrator;

/**
 * Created by robertlindquist on 6/27/17.
 */
public class H2Middleware extends HermarMiddleware {

    private float gamma;

    public H2Middleware(float gamma, int k, int maxIterations, NoRepManager manager) {
        super(gamma, k, maxIterations, manager);
        this.gamma = gamma;
    }

    @Override
    public void removePartition(Integer pid) {
        NoRepManager manager = getManager();
        TIntIntMap targets = HMigrator.migrateOffPartition(pid, gamma, manager.getPartitionToUsers(), manager.getFriendships());
        for(Integer uid : targets.keys()) {
            manager.moveUser(uid, targets.get(uid), true);
        }
        manager.removePartition(pid);
    }
}
