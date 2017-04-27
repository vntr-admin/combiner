package io.vntr.middleware;

import io.vntr.User;
import io.vntr.befriend.JBefriender;
import io.vntr.manager.NoRepManager;
import io.vntr.repartition.JRepartitioner;
import io.vntr.repartition.NoRepResults;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class JabarMiddleware extends AbstractNoRepMiddleware{
    private float alpha;
    private float initialT;
    private float deltaT;
    private int k;
    private NoRepManager manager;

    public JabarMiddleware(float alpha, float initialT, float deltaT, int k, NoRepManager manager) {
        super(manager);
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        this.manager = manager;
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        super.befriend(smallerUserId, largerUserId);
        rebalance(smallerUserId, largerUserId);
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

    void rebalance(Integer smallerUserId, Integer largerUserId) {
        JBefriender.Result result = JBefriender.rebalance(smallerUserId, largerUserId, k, alpha, getFriendships(), manager.getPartitionToUsers());
        Integer uid1 = result.getUid1();
        Integer uid2 = result.getUid2();
        if(uid1 != null && uid2 != null) {
            int pid1 = manager.getPidForUser(uid1);
            int pid2 = manager.getPidForUser(uid2);
            manager.moveUser(uid1, pid2, false);
            manager.moveUser(uid2, pid1, false);
        }
    }

    public void repartition() {
        NoRepResults noRepResults = JRepartitioner.repartition(alpha, initialT, deltaT, k, 1, getPartitionToUserMap(), getFriendships(), true);
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
