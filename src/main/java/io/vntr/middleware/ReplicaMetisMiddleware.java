package io.vntr.middleware;

import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.manager.RepManager;
import io.vntr.repartition.ReplicaMetisRepartitioner;
import io.vntr.repartition.RepResults;
import io.vntr.utils.TroveUtils;
import io.vntr.utils.Utils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 6/4/17.
 */
public class ReplicaMetisMiddleware extends SparMiddleware {
    private final String gpmetisLocation;
    private final String gpmetisTempdir;
    private int minNumReplicas;

    public ReplicaMetisMiddleware(RepManager manager, String gpmetisLocation, String gpmetisTempdir, int minNumReplicas) {
        super(manager);
        this.gpmetisLocation = gpmetisLocation;
        this.gpmetisTempdir = gpmetisTempdir;
        this.minNumReplicas = minNumReplicas;
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        super.befriend(smallerUserId, largerUserId);
        if(Math.random() > .9) {
            repartition();
        }
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    void repartition() {
        clearReplicas();
        RepResults repResults = ReplicaMetisRepartitioner.repartition(gpmetisLocation, gpmetisTempdir, TroveUtils.convertMapSetToTIntObjectMapTIntSet(getFriendships()), new TIntHashSet(getPartitionIds()), minNumReplicas);
        getManager().increaseTallyLogical(repResults.getNumLogicalMoves());
        physicallyMigrate(repResults.getUidToPidMap(), repResults.getUidsToReplicaPids());
    }

    void clearReplicas() {
        Map<Integer, Set<Integer>> replicaLocations = Utils.copyMapSet(Utils.getUToReplicasMap(getPartitionToReplicasMap(), getUserIds()));
        for(Integer uid : replicaLocations.keySet()) {
            for(Integer replicaPid : replicaLocations.get(uid)) {
                getManager().removeReplica(getManager().getUserMaster(uid), replicaPid);
            }
        }
    }

    void physicallyMigrate(Map<Integer, Integer> newPids, Map<Integer, Set<Integer>> newReplicaPids) {
        for(Integer uid : newPids.keySet()) {
            Integer newPid = newPids.get(uid);
            Set<Integer> newReplicas = newReplicaPids.get(uid);

            RepUser user = getManager().getUserMaster(uid);
            Integer oldPid = user.getBasePid();

            if(!oldPid.equals(newPid)) {
                getManager().moveMasterAndInformReplicas(uid, user.getBasePid(), newPid);
                getManager().increaseTally(1);
            }

            for(Integer replicaPid : newReplicas) {
                getManager().addReplica(user, replicaPid);
            }
        }
    }
}
