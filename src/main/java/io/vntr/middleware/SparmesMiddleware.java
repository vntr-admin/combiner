package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.repartition.RepResults;
import io.vntr.repartition.SparmesRepartitioner;
import io.vntr.manager.RepManager;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesMiddleware extends SparMiddleware {

    private final int minNumReplicas;
    private final int maxIterations;
    private final float gamma;
    private final int k;

    public SparmesMiddleware(int minNumReplicas, float gamma, int k, int maxIterations, RepManager manager) {
        super(manager);
        this.minNumReplicas = minNumReplicas;
        this.gamma = gamma;
        this.k = k;
        this.maxIterations = maxIterations;
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    void repartition() {
        RepResults repResults = SparmesRepartitioner.repartition(k, maxIterations, gamma, minNumReplicas, getManager().getPartitionToUserMap(), getManager().getPartitionToReplicasMap(), getManager().getFriendships());
        getManager().increaseTallyLogical(repResults.getNumLogicalMoves());
        physicallyMigrate(repResults);
    }

    void physicallyMigrate(RepResults repResults) {
        TIntIntMap currentUidToPidMap = getUToMasterMap(getManager().getPartitionToUserMap());
        TIntObjectMap<TIntSet> currentUidToReplicasMap = getUToReplicasMap(getManager().getPartitionToReplicasMap(), new TIntHashSet(getManager().getUids()));

        for(TIntIterator iter = getManager().getUids().iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            int currentPid = currentUidToPidMap.get(uid);
            int newPid = repResults.getUidToPidMap().get(uid);

            //move master
            if(currentPid != newPid) {
                getManager().moveMasterAndInformReplicas(uid, currentPid, newPid);
                getManager().increaseTally(1);
            }

            //add and remove replicas as specified in the repResults
            TIntSet newReplicas = repResults.getUidsToReplicaPids().get(uid);
            TIntSet currentReplicas = currentUidToReplicasMap.get(uid);
            if(!currentReplicas.equals(newReplicas)) {
                for(TIntIterator iter2 = newReplicas.iterator(); iter2.hasNext(); ) {
                    int newReplica = iter2.next();
                    if(!currentReplicas.contains(newReplica)) {
                        getManager().addReplica(getManager().getUserMaster(uid), newReplica);
                    }
                }
                for(TIntIterator iter2 = currentReplicas.iterator(); iter2.hasNext(); ) {
                    int oldReplica = iter2.next();
                    if(!newReplicas.contains(oldReplica)) {
                        getManager().removeReplica(getManager().getUserMaster(uid), oldReplica);
                    }
                }
            }
        }

        //shore up friend replicas (ideally would be unnecessary)
        for(TIntIterator iter = getManager().getUids().iterator(); iter.hasNext(); ) {
            shoreUpFriendReplicas(iter.next());
        }

        //ensure k replication
        for(TIntIterator iter = getManager().getUids().iterator(); iter.hasNext(); ) {
            ensureKReplication(iter.next());
        }
    }

    void shoreUpFriendReplicas(int uid) {
        RepUser user = getManager().getUserMaster(uid);
        int pid = user.getBasePid();
        TIntSet friends = new TIntHashSet(user.getFriendIDs());
        friends.removeAll(getManager().getMastersOnPartition(pid));
        friends.removeAll(getManager().getReplicasOnPartition(pid));
        for(TIntIterator iter = friends.iterator(); iter.hasNext(); ) {
            getManager().addReplica(getManager().getUserMaster(iter.next()), pid);
        }
    }

    void ensureKReplication(int uid) {
        RepUser user = getManager().getUserMaster(uid);
        TIntSet replicaLocations = user.getReplicaPids();
        int deficit = minNumReplicas - replicaLocations.size();
        if(deficit > 0) {
            TIntSet newLocations = new TIntHashSet(getPartitionIds());
            newLocations.removeAll(replicaLocations);
            newLocations.remove(user.getBasePid());
            TIntSet newReplicaPids = getKDistinctValuesFromArray(deficit, newLocations.toArray());
            for(TIntIterator iter = newReplicaPids.iterator(); iter.hasNext(); ) {
                getManager().addReplica(user, iter.next());
            }
        }
    }

}
