package io.vntr.middleware;

import io.vntr.RepUser;
import io.vntr.manager.RepManager;
import io.vntr.repartition.ReplicaMetisRepartitioner;
import io.vntr.repartition.RepResults;

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
        RepResults repResults = ReplicaMetisRepartitioner.repartition(gpmetisLocation, gpmetisTempdir, getFriendships(), getPartitionIds(), minNumReplicas);
        getManager().increaseTallyLogical(repResults.getNumLogicalMoves());
        physicallyMigrate(repResults.getUidToPidMap(), repResults.getUidsToReplicaPids());
    }

    void physicallyMigrate(Map<Integer, Integer> newPids, Map<Integer, Set<Integer>> newReplicaPids) {
        for(Integer uid : newPids.keySet()) {
            Integer newPid = newPids.get(uid);
            Set<Integer> newReplicas = newReplicaPids.get(uid);

            RepUser user = getManager().getUserMaster(uid);
            Integer oldPid = user.getBasePid();
            Set<Integer> oldReplicas = user.getReplicaPids();

            if(!oldPid.equals(newPid)) {
                getManager().moveMasterAndInformReplicas(uid, user.getBasePid(), newPid);
                getManager().increaseTally(1);
            }

            if(!oldReplicas.equals(newReplicas)) {
                Set<Integer> replicasToAdd = new HashSet<>(newReplicas);
                replicasToAdd.removeAll(oldReplicas);
                for(Integer replicaPid : replicasToAdd) {
                    getManager().addReplica(user, replicaPid);
                }

                Set<Integer> replicasToRemove = new HashSet<>(oldReplicas);
                replicasToRemove.removeAll(newReplicas);
                for(Integer replicaPid : replicasToRemove) {
                    getManager().removeReplica(user, replicaPid);
                }
            }
        }
    }
}
