package io.vntr.middleware;

import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.migration.HMigrator;
import io.vntr.repartition.HRepartitioner;
import io.vntr.repartition.NoRepResults;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesMiddleware extends AbstractNoRepMiddleware {
    private final float gamma;
    private final int k;
    private final int maxIterations;
    private NoRepManager manager;

    public HermesMiddleware(float gamma, int k, int maxIterations, NoRepManager manager) {
        super(manager);
        this.gamma = gamma;
        this.k = k;
        this.maxIterations = maxIterations;
        this.manager = manager;
    }

    @Override
    public void addUser(User user) {
        super.addUser(user);
        repartition();
    }

    @Override
    public void removePartition(Integer partitionId) {
        Map<Integer, Integer> targets = HMigrator.migrateOffPartition(partitionId, gamma, manager.getPartitionToUsers(), manager.getFriendships());
        for(Integer uid : targets.keySet()) {
            manager.moveUser(uid, targets.get(uid), true);
        }
        manager.removePartition(partitionId);
    }

    @Override
    public void broadcastDowntime() {
        //Hermes only repartitions upon node addition or node weight change (the latter of which we do not consider)
    }

    public void repartition() {
        NoRepResults noRepResults = HRepartitioner.repartition(k, maxIterations, gamma, manager.getPartitionToUsers(), getFriendships());
        int numMoves = noRepResults.getLogicalMoves();
        if(numMoves > 0) {
            manager.increaseTallyLogical(numMoves);
            physicallyMigrate(noRepResults.getUidsToPids());
        }
    }

    void physicallyMigrate(Map<Integer, Integer> uidsToPids) {
        for(int uid : uidsToPids.keySet()) {
            int pid = uidsToPids.get(uid);
            User user = manager.getUser(uid);
            if(user.getBasePid() != pid) {
                manager.moveUser(uid, pid, false);
            }
        }
    }
}
