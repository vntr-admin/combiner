package io.vntr.middleware;

import java.util.*;

import io.vntr.RepUser;
import io.vntr.manager.RepManager;
import io.vntr.repartition.RepResults;
import io.vntr.repartition.SpajaRepartitioner;

public class SpajaMiddleware extends SparMiddleware {
    private int minNumReplicas;
    private float alpha;
    private float initialT;
    private float deltaT;
    private int k;

    public SpajaMiddleware(int minNumReplicas, float alpha, float initialT, float deltaT, int k, RepManager manager) {
        super(manager);
        this.minNumReplicas = minNumReplicas;
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    void repartition() {
        RepResults repResults = SpajaRepartitioner.repartition(minNumReplicas, alpha, initialT, deltaT, k, getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap());
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
