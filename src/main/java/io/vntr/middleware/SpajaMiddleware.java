package io.vntr.middleware;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.RepUser;
import io.vntr.manager.RepManager;
import io.vntr.repartition.RepResults;
import io.vntr.repartition.SpajaRepartitioner;

public class SpajaMiddleware extends SparMiddleware {
    private final short minNumReplicas;
    private final float alpha;
    private final float initialT;
    private final float deltaT;
    private final short k;

    public SpajaMiddleware(short minNumReplicas, float alpha, float initialT, float deltaT, short k, RepManager manager) {
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

    void physicallyMigrate(TShortShortMap newPids, TShortObjectMap<TShortSet> newReplicaPids) {
        for(short uid : newPids.keys()) {
            short newPid = newPids.get(uid);
            TShortSet newReplicas = newReplicaPids.get(uid);

            RepUser user = getManager().getUserMaster(uid);
            short oldPid = user.getBasePid();
            TShortSet oldReplicas = user.getReplicaPids();

            if(oldPid != (newPid)) {
                getManager().moveMasterAndInformReplicas(uid, user.getBasePid(), newPid);
                getManager().increaseTally(1);
            }

            if(!oldReplicas.equals(newReplicas)) {
                TShortSet replicasToAdd = new TShortHashSet(newReplicas);
                replicasToAdd.removeAll(oldReplicas);
                for(TShortIterator iter = replicasToAdd.iterator(); iter.hasNext(); ) {
                    getManager().addReplica(user, iter.next());
                }

                TShortSet replicasToRemove = new TShortHashSet(oldReplicas);
                replicasToRemove.removeAll(newReplicas);
                for(TShortIterator iter = replicasToRemove.iterator(); iter.hasNext(); ) {
                    getManager().removeReplica(user, iter.next());
                }
            }
        }
    }

}
