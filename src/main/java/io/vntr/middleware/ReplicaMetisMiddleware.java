package io.vntr.middleware;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.RepUser;
import io.vntr.manager.RepManager;
import io.vntr.repartition.ReplicaMetisRepartitioner;
import io.vntr.repartition.RepResults;

import static io.vntr.utils.TroveUtils.*;


/**
 * Created by robertlindquist on 6/4/17.
 */
public class ReplicaMetisMiddleware extends SparMiddleware {
    private final String gpmetisLocation;
    private final String gpmetisTempdir;
    private short minNumReplicas;

    public ReplicaMetisMiddleware(RepManager manager, String gpmetisLocation, String gpmetisTempdir, short minNumReplicas) {
        super(manager);
        this.gpmetisLocation = gpmetisLocation;
        this.gpmetisTempdir = gpmetisTempdir;
        this.minNumReplicas = minNumReplicas;
    }

    @Override
    public void befriend(short smallerUid, short largerUid) {
        super.befriend(smallerUid, largerUid);
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
        RepResults repResults = ReplicaMetisRepartitioner.repartition(gpmetisLocation, gpmetisTempdir, getManager().getFriendships(), new TShortHashSet(getManager().getPids()), minNumReplicas);
        getManager().increaseTallyLogical(repResults.getNumLogicalMoves());
        physicallyMigrate(repResults.getUidToPidMap(), repResults.getUidsToReplicaPids());
    }

    void clearReplicas() {
        TShortObjectMap<TShortSet> replicaLocations = copyTShortObjectMapIntSet(getUToReplicasMap(getManager().getPartitionToReplicasMap(), new TShortHashSet(getManager().getUids())));
        for(short uid : replicaLocations.keys()) {
            for(TShortIterator iter = replicaLocations.get(uid).iterator(); iter.hasNext(); ) {
                getManager().removeReplica(getManager().getUserMaster(uid), iter.next());
            }
        }
    }

    void physicallyMigrate(TShortShortMap newPids, TShortObjectMap<TShortSet> newReplicaPids) {
        for(short uid : newPids.keys()) {
            short newPid = newPids.get(uid);
            TShortSet newReplicas = newReplicaPids.get(uid);

            RepUser user = getManager().getUserMaster(uid);
            short oldPid = user.getBasePid();

            if(oldPid != newPid) {
                getManager().moveMasterAndInformReplicas(uid, user.getBasePid(), newPid);
                getManager().increaseTally(1);
            }

            for(TShortIterator iter = newReplicas.iterator(); iter.hasNext(); ) {
                getManager().addReplica(user, iter.next());
            }
        }
    }
}
