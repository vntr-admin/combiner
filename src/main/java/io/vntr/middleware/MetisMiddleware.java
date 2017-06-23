package io.vntr.middleware;

import gnu.trove.set.hash.TIntHashSet;
import io.vntr.manager.NoRepManager;
import io.vntr.repartition.MetisRepartitioner;
import io.vntr.utils.ProbabilityUtils;
import io.vntr.utils.TroveUtils;

import java.util.*;

/**
 * Created by robertlindquist on 12/5/16.
 */
public class MetisMiddleware extends AbstractNoRepMiddleware {

    private final String gpmetisLocation;
    private final String gpmetisTempdir;

    private NoRepManager manager;

    public MetisMiddleware(String gpmetisLocation, String gpmetisTempdir, NoRepManager manager) {
        super(manager);
        this.gpmetisLocation = gpmetisLocation;
        this.gpmetisTempdir = gpmetisTempdir;
        this.manager = manager;
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        super.befriend(smallerUserId, largerUserId);
        if(Math.random() > .9) {
            repartition();
        }
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

    void repartition() {
        Map<Integer, Integer> newPartitioning = MetisRepartitioner.partition(gpmetisLocation, gpmetisTempdir, TroveUtils.convertMapSetToTIntObjectMapTIntSet(getFriendships()), new TIntHashSet(getPartitionToUserMap().keySet()));
        for(int uid : newPartitioning.keySet()) {
            int newPid = newPartitioning.get(uid);
            if(newPid != manager.getUser(uid).getBasePid()) {
                manager.moveUser(uid, newPid, false);
            }
        }
    }


}
