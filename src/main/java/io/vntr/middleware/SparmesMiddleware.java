package io.vntr.middleware;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.RepUser;
import io.vntr.repartition.RepResults;
import io.vntr.repartition.SparmesRepartitioner;
import io.vntr.manager.RepManager;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesMiddleware extends SparMiddleware {

    private final short minNumReplicas;
    private final short maxIterations;
    private final float gamma;
    private final short k;

    public SparmesMiddleware(short minNumReplicas, float gamma, short k, short maxIterations, RepManager manager) {
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
        TShortShortMap currentUidToPidMap = getUToMasterMap(getManager().getPartitionToUserMap());
        TShortObjectMap<TShortSet> currentUidToReplicasMap = getUToReplicasMap(getManager().getPartitionToReplicasMap(), new TShortHashSet(getManager().getUids()));

        for(TShortIterator iter = getManager().getUids().iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            short currentPid = currentUidToPidMap.get(uid);
            short newPid = repResults.getUidToPidMap().get(uid);

            //move master
            if(currentPid != newPid) {
                getManager().moveMasterAndInformReplicas(uid, currentPid, newPid);
                getManager().increaseTally(1);
            }

            //add and remove replicas as specified in the repResults
            TShortSet newReplicas = repResults.getUidsToReplicaPids().get(uid);
            TShortSet currentReplicas = currentUidToReplicasMap.get(uid);
            if(!currentReplicas.equals(newReplicas)) {
                for(TShortIterator iter2 = newReplicas.iterator(); iter2.hasNext(); ) {
                    short newReplica = iter2.next();
                    if(!currentReplicas.contains(newReplica)) {
                        getManager().addReplica(getManager().getUserMaster(uid), newReplica);
                    }
                }
                for(TShortIterator iter2 = currentReplicas.iterator(); iter2.hasNext(); ) {
                    short oldReplica = iter2.next();
                    if(!newReplicas.contains(oldReplica)) {
                        getManager().removeReplica(getManager().getUserMaster(uid), oldReplica);
                    }
                }
            }
        }

        //shore up friend replicas (ideally would be unnecessary)
        for(TShortIterator iter = getManager().getUids().iterator(); iter.hasNext(); ) {
            shoreUpFriendReplicas(iter.next());
        }

        //ensure k replication
        for(TShortIterator iter = getManager().getUids().iterator(); iter.hasNext(); ) {
            ensureKReplication(iter.next());
        }
    }

    void shoreUpFriendReplicas(short uid) {
        RepUser user = getManager().getUserMaster(uid);
        short pid = user.getBasePid();
        TShortSet friends = new TShortHashSet(user.getFriendIDs());
        friends.removeAll(getManager().getMastersOnPartition(pid));
        friends.removeAll(getManager().getReplicasOnPartition(pid));
        for(TShortIterator iter = friends.iterator(); iter.hasNext(); ) {
            getManager().addReplica(getManager().getUserMaster(iter.next()), pid);
        }
    }

    void ensureKReplication(short uid) {
        RepUser user = getManager().getUserMaster(uid);
        TShortSet replicaLocations = user.getReplicaPids();
        short deficit = (short)(minNumReplicas - replicaLocations.size());
        if(deficit > 0) {
            TShortSet newLocations = new TShortHashSet(getPids());
            newLocations.removeAll(replicaLocations);
            newLocations.remove(user.getBasePid());
            TShortSet newReplicaPids = getKDistinctValuesFromArray(deficit, newLocations.toArray());
            for(TShortIterator iter = newReplicaPids.iterator(); iter.hasNext(); ) {
                getManager().addReplica(user, iter.next());
            }
        }
    }

}
