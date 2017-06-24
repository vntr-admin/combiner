package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.manager.RepManager;
import io.vntr.repartition.RepResults;
import io.vntr.repartition.SpajaRepartitioner;

public class SpajaMiddleware extends SparMiddleware {
    private final int minNumReplicas;
    private final float alpha;
    private final float initialT;
    private final float deltaT;
    private final int k;

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
        RepResults repResults = SpajaRepartitioner.repartition(minNumReplicas, alpha, initialT, deltaT, k, getManager().getFriendships(), getManager().getPartitionToUserMap(), getManager().getPartitionToReplicasMap());
        getManager().increaseTallyLogical(repResults.getNumLogicalMoves());
        physicallyMigrate(repResults.getUidToPidMap(), repResults.getUidsToReplicaPids());
    }

    void physicallyMigrate(TIntIntMap newPids, TIntObjectMap<TIntSet> newReplicaPids) {
        for(Integer uid : newPids.keys()) {
            Integer newPid = newPids.get(uid);
            TIntSet newReplicas = newReplicaPids.get(uid);

            RepUser user = getManager().getUserMaster(uid);
            Integer oldPid = user.getBasePid();
            TIntSet oldReplicas = user.getReplicaPids();

            if(!oldPid.equals(newPid)) {
                getManager().moveMasterAndInformReplicas(uid, user.getBasePid(), newPid);
                getManager().increaseTally(1);
            }

            if(!oldReplicas.equals(newReplicas)) {
                TIntSet replicasToAdd = new TIntHashSet(newReplicas);
                replicasToAdd.removeAll(oldReplicas);
                for(TIntIterator iter = replicasToAdd.iterator(); iter.hasNext(); ) {
                    int replicaPid = iter.next();
                    getManager().addReplica(user, replicaPid);
                }

                TIntSet replicasToRemove = new TIntHashSet(oldReplicas);
                replicasToRemove.removeAll(newReplicas);
                for(TIntIterator iter = replicasToRemove.iterator(); iter.hasNext(); ) {
                    getManager().removeReplica(user, iter.next());
                }
            }
        }
    }

}
