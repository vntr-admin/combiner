package io.vntr.middleware;

import gnu.trove.map.TShortShortMap;
import io.vntr.manager.NoRepManager;
import io.vntr.migration.HMigrator;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class J2Middleware extends JabarMiddleware {

    public J2Middleware(float alpha, float initialT, float deltaT, short k, NoRepManager manager) {
        super(alpha, initialT, deltaT, k, manager);
    }

    @Override
    public void removePartition(short pid) {
        TShortShortMap targets = HMigrator.migrateOffPartition(pid, 1f, getManager().getPartitionToUsers(), getManager().getFriendships());
        for(short uid : targets.keys()) {
            getManager().moveUser(uid, targets.get(uid), true);
        }
        getManager().removePartition(pid);
    }
}
