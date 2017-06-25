package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
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
    private int minNumReplicas;

    public ReplicaMetisMiddleware(RepManager manager, String gpmetisLocation, String gpmetisTempdir, int minNumReplicas) {
        super(manager);
        this.gpmetisLocation = gpmetisLocation;
        this.gpmetisTempdir = gpmetisTempdir;
        this.minNumReplicas = minNumReplicas;
    }

    @Override
    public void befriend(Integer smallerUid, Integer largerUid) {
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
        RepResults repResults = ReplicaMetisRepartitioner.repartition(gpmetisLocation, gpmetisTempdir, getManager().getFriendships(), new TIntHashSet(getManager().getPids()), minNumReplicas);
        getManager().increaseTallyLogical(repResults.getNumLogicalMoves());
        physicallyMigrate(repResults.getUidToPidMap(), repResults.getUidsToReplicaPids());
    }

    void clearReplicas() {
        TIntObjectMap<TIntSet> replicaLocations = copyTIntObjectMapIntSet(getUToReplicasMap(getManager().getPartitionToReplicasMap(), new TIntHashSet(getManager().getUids())));
        for(Integer uid : replicaLocations.keys()) {
            for(TIntIterator iter = replicaLocations.get(uid).iterator(); iter.hasNext(); ) {
                getManager().removeReplica(getManager().getUserMaster(uid), iter.next());
            }
        }
    }

    void physicallyMigrate(TIntIntMap newPids, TIntObjectMap<TIntSet> newReplicaPids) {
        for(Integer uid : newPids.keys()) {
            Integer newPid = newPids.get(uid);
            TIntSet newReplicas = newReplicaPids.get(uid);

            RepUser user = getManager().getUserMaster(uid);
            Integer oldPid = user.getBasePid();

            if(!oldPid.equals(newPid)) {
                getManager().moveMasterAndInformReplicas(uid, user.getBasePid(), newPid);
                getManager().increaseTally(1);
            }

            for(TIntIterator iter = newReplicas.iterator(); iter.hasNext(); ) {
                getManager().addReplica(user, iter.next());
            }
        }
    }
}
