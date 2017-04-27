package io.vntr.middleware;

import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.repartition.JRepartitioner;
import io.vntr.repartition.NoRepResults;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class JabejaMiddleware extends AbstractNoRepMiddleware {
    private float alpha;
    private float initialT;
    private float deltaT;
    private int k;
    private int numRestarts;
    private NoRepManager manager;

    public JabejaMiddleware(float alpha, float initialT, float deltaT, int k, int numRestarts, NoRepManager manager) {
        super(manager);
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        this.numRestarts = numRestarts;
        this.manager = manager;
    }

    @Override
    public void removePartition(Integer partitionId) {
        Set<Integer> partition = manager.getPartition(partitionId);
        manager.removePartition(partitionId);
        for(Integer uid : partition) {
            Integer newPid = ProbabilityUtils.getRandomElement(manager.getPids());
            manager.moveUser(uid, newPid, true);
        }
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    public void repartition() {
        NoRepResults noRepResults = JRepartitioner.repartition(alpha, initialT, deltaT, k, numRestarts, getPartitionToUserMap(), getFriendships(), false);
        manager.increaseTallyLogical(noRepResults.getLogicalMoves());
        if(noRepResults.getUidsToPids() != null) {
            physicallyMigrate(noRepResults.getUidsToPids());
        }
    }

    void physicallyMigrate(Map<Integer, Integer> logicalPids) {
        for(Integer uid : logicalPids.keySet()) {
            User user = manager.getUser(uid);
            Integer newPid = logicalPids.get(uid);
            if(!user.getBasePid().equals(newPid)) {
                manager.moveUser(uid, newPid, false);
            }
        }
    }
}
