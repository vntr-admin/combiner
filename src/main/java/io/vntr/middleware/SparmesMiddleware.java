package io.vntr.middleware;

import io.vntr.RepUser;
import io.vntr.repartition.RepResults;
import io.vntr.repartition.SparmesRepartitioner;
import io.vntr.manager.RepManager;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.utils.Utils.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesMiddleware extends SparMiddleware {

    private int minNumReplicas;
    private float gamma;
    private int k;

    public SparmesMiddleware(int minNumReplicas, float gamma, int k, RepManager manager) {
        super(manager);
        this.minNumReplicas = minNumReplicas;
        this.gamma = gamma;
        this.k = k;
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    public void repartition() {
        RepResults repResults = SparmesRepartitioner.repartition(k, 100, gamma, minNumReplicas, getPartitionToUserMap(), getPartitionToReplicasMap(), getFriendships());
        getManager().increaseTallyLogical(repResults.getNumLogicalMoves());
        physicallyMigrate(repResults);
    }

    void physicallyMigrate(RepResults repResults) {
        Map<Integer, Integer> currentUidToPidMap = getUToMasterMap(getPartitionToUserMap());
        Map<Integer, Set<Integer>> currentUidToReplicasMap = getUToReplicasMap(getPartitionToReplicasMap(), getUserIds());

        for(int uid : getManager().getUids()) {
            int currentPid = currentUidToPidMap.get(uid);
            int newPid = repResults.getUidToPidMap().get(uid);

            //move master
            if(currentPid != newPid) {
                getManager().moveMasterAndInformReplicas(uid, currentPid, newPid);
                getManager().increaseTally(1);
            }

            //add and remove replicas as specified in the repResults
            Set<Integer> newReplicas = repResults.getUidsToReplicaPids().get(uid);
            Set<Integer> currentReplicas = currentUidToReplicasMap.get(uid);
            if(!currentReplicas.equals(newReplicas)) {
                for(int newReplica : newReplicas) {
                    if(!currentReplicas.contains(newReplica)) {
                        getManager().addReplica(getManager().getUserMaster(uid), newReplica);
                    }
                }
                for(int oldReplica : currentReplicas) {
                    if(!newReplicas.contains(oldReplica)) {
                        getManager().removeReplica(getManager().getUserMaster(uid), oldReplica);
                    }
                }
            }
        }

        //shore up friend replicas (ideally would be unnecessary)
        for(int uid : getManager().getUids()) {
            shoreUpFriendReplicas(uid);
        }

        //ensure k replication
        for(int uid : getManager().getUids()) {
            ensureKReplication(uid);
        }
    }

    void shoreUpFriendReplicas(int uid) {
        RepUser user = getManager().getUserMaster(uid);
        int pid = user.getBasePid();
        Set<Integer> friends = new HashSet<>(user.getFriendIDs());
        friends.removeAll(getManager().getMastersOnPartition(pid));
        friends.removeAll(getManager().getReplicasOnPartition(pid));
        for(int friendId : friends) {
            getManager().addReplica(getManager().getUserMaster(friendId), pid);
        }
    }

    void ensureKReplication(int uid) {
        RepUser user = getManager().getUserMaster(uid);
        Set<Integer> replicaLocations = user.getReplicaPids();
        int deficit = minNumReplicas - replicaLocations.size();
        if(deficit > 0) {
            Set<Integer> newLocations = new HashSet<>(getPartitionIds());
            newLocations.removeAll(replicaLocations);
            newLocations.remove(user.getBasePid());
            for(int rPid : ProbabilityUtils.getKDistinctValuesFromList(deficit, newLocations)) {
                getManager().addReplica(user, rPid);
            }
        }
    }

}
