package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/15/17.
 */
public class SpajaRepartitioner {

    public static RepResults repartition(int minNumReplicas, float alpha, float initialT, float deltaT, int k, TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas) {
        int logicalMoves = 0;
        int initialNumReplicas = getLogicalReplicationCount(replicas);

        TIntIntMap bestLogicalPids = new TIntIntHashMap(partitions.size()+1);
        TIntObjectMap<TIntSet> bestLogicalReplicaPids = new TIntObjectHashMap<>(partitions.size()+1);
        State state = getState(minNumReplicas, alpha, initialT, deltaT, k, friendships, partitions, replicas);

        for(float t = initialT; t >= 1; t -= deltaT) {
            int[] randomUserIdArray = new int[friendships.size()];
            System.arraycopy(friendships.keys(), 0, randomUserIdArray, 0, friendships.size());
            shuffle(randomUserIdArray);
            for (Integer uid : randomUserIdArray) {
                TIntSet swapCandidates = getKDistinctValuesFromArray(k, friendships.keys());

                Integer partnerId = findPartner(uid, swapCandidates, t, state);
                if(partnerId != null) {
                    swap(uid, partnerId, state);
                    logicalMoves += 2;
                }
            }
        }

        int numReplicas = getLogicalReplicationCount(state.getLogicalReplicaPartitions());
        if(numReplicas < initialNumReplicas) {
            bestLogicalPids = new TIntIntHashMap(state.getLogicalPids());
            bestLogicalReplicaPids = new TIntObjectHashMap<>(state.getLogicalReplicaPids());
        }

        RepResults repResults = new RepResults(logicalMoves, bestLogicalPids, bestLogicalReplicaPids);
        return repResults;
    }

    static Integer findPartner(Integer uid, TIntSet candidateIds, float t, State state) {
        Integer bestPartnerId = null;
        float bestScore = Float.MAX_VALUE;

        Integer logicalPid = state.getLogicalPids().get(uid);
        for(TIntIterator iter = candidateIds.iterator(); iter.hasNext(); ) {
            int partnerId = iter.next();
            Integer partnerLogicalPid = state.getLogicalPids().get(partnerId);
            if(logicalPid.equals(partnerLogicalPid)) {
                continue;
            }

            try {

                int mine = state.getLogicalReplicaPartitions().get(logicalPid).size();
                int theirs = state.getLogicalReplicaPartitions().get(partnerLogicalPid).size();

                SwapChanges swapChanges = getSwapChanges(uid, partnerId, state);

                int deltaMine   = swapChanges.getAddToP1().size() - swapChanges.getRemoveFromP1().size();
                int deltaTheirs = swapChanges.getAddToP2().size() - swapChanges.getRemoveFromP2().size();

                float oldScore = calcScore(mine,             theirs,               state.getAlpha());
                float newScore = calcScore(mine + deltaMine, theirs + deltaTheirs, state.getAlpha());

                if(newScore < bestScore && (newScore / t) < oldScore) {
                    bestPartnerId = partnerId;
                    bestScore = newScore;
                }
            } catch(Exception e) {
                System.out.println("Yowzers!");
            }


        }

        return bestPartnerId;
    }

    static Integer getLogicalReplicationCount(TIntObjectMap<TIntSet> replicas) {
        int count = 0;
        for(Object reps : replicas.values()) {
            count += ((TIntSet) reps).size();
        }
        return count;
    }

    static State getState(int minNumReplicas, float alpha, float initialT, float deltaT, int k, TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas) {
        State state = new State(minNumReplicas, alpha, initialT, deltaT, k, friendships);

        state.setLogicalPids(getUToMasterMap(partitions));
        state.setLogicalReplicaPids(getUToReplicasMap(replicas, friendships.keySet()));

        TIntObjectMap<TIntSet> logicalReplicaPartitions = new TIntObjectHashMap<>(partitions.size()+1);
        for(Integer pid : partitions.keys()) {
            logicalReplicaPartitions.put(pid, new TIntHashSet(replicas.get(pid)));
        }
        state.setLogicalReplicaPartitions(logicalReplicaPartitions);

        return state;
    }

    static float calcScore(int replicasInP1, int replicasInP2, float alpha) {
        return (float)(Math.pow(replicasInP1, alpha) + Math.pow(replicasInP2, alpha));
    }

    static void swap(Integer uid1, Integer uid2, State state) {
        SwapChanges swapChanges = getSwapChanges(uid1, uid2, state);

        Integer pid1 = state.getLogicalPids().get(uid1);
        Integer pid2 = state.getLogicalPids().get(uid2);

        state.getLogicalPids().put(uid1, pid2);
        state.getLogicalPids().put(uid2, pid1);

        for(TIntIterator iter = swapChanges.getAddToP1().iterator(); iter.hasNext(); ) {
            state.getLogicalReplicaPids().get(iter.next()).add(pid1);
        }
        for(TIntIterator iter = swapChanges.getAddToP2().iterator(); iter.hasNext(); ) {
            state.getLogicalReplicaPids().get(iter.next()).add(pid2);
        }
        for(TIntIterator iter = swapChanges.getRemoveFromP1().iterator(); iter.hasNext(); ) {
            state.getLogicalReplicaPids().get(iter.next()).remove(pid1);
        }
        for(TIntIterator iter = swapChanges.getRemoveFromP2().iterator(); iter.hasNext(); ) {
            state.getLogicalReplicaPids().get(iter.next()).remove(pid2);
        }

        state.getLogicalReplicaPartitions().get(pid1).addAll(swapChanges.getAddToP1());
        state.getLogicalReplicaPartitions().get(pid2).addAll(swapChanges.getAddToP2());

        state.getLogicalReplicaPartitions().get(pid1).removeAll(swapChanges.getRemoveFromP1());
        state.getLogicalReplicaPartitions().get(pid2).removeAll(swapChanges.getRemoveFromP2());
    }

    static SwapChanges getSwapChanges(Integer uid1, Integer uid2, State state) {
        TIntSet u1Friends = state.getFriendships().get(uid1);
        TIntSet u2Friends = state.getFriendships().get(uid2);

        Integer pid1 = state.getLogicalPids().get(uid1);
        Integer pid2 = state.getLogicalPids().get(uid2);

        boolean u1AndU2AreFriends = u1Friends.contains(uid2);

        TIntSet mutualFriends = new TIntHashSet(u1Friends);
        mutualFriends.retainAll(u2Friends);

        SwapChanges swapChanges = new SwapChanges();
        swapChanges.setPid1(pid1);
        swapChanges.setPid2(pid2);

        TIntSet addToP1 = findReplicasToAddToTargetPartition(uid2, pid1, state);
        if(u1AndU2AreFriends || shouldWeAddAReplicaOfMovingUserInMovingPartition(uid1, pid2, state)) {
            addToP1.add(uid1);
        }
        swapChanges.setAddToP1(addToP1);

        TIntSet addToP2 = findReplicasToAddToTargetPartition(uid1, pid2, state);
        if(u1AndU2AreFriends || shouldWeAddAReplicaOfMovingUserInMovingPartition(uid2, pid1, state)) {
            addToP2.add(uid2);
        }
        swapChanges.setAddToP2(addToP2);

        TIntSet removeFromP1 = findReplicasInMovingPartitionToDelete(uid1, pid1, addToP1, state);
        if(shouldDeleteReplicaInTargetPartition(uid2, pid1, state)) {
            removeFromP1.add(uid2);
        }
        removeFromP1.removeAll(mutualFriends);
        swapChanges.setRemoveFromP1(removeFromP1);

        TIntSet removeFromP2 = findReplicasInMovingPartitionToDelete(uid2, pid2, addToP2, state);
        if(shouldDeleteReplicaInTargetPartition(uid1, pid2, state)) {
            removeFromP2.add(uid1);
        }
        removeFromP2.removeAll(mutualFriends);
        swapChanges.setRemoveFromP2(removeFromP2);

        return swapChanges;
    }

    static TIntSet findReplicasToAddToTargetPartition(Integer uid, Integer targetPid, State state) {
        TIntSet replicasToAdd = new TIntHashSet();
        for(TIntIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            int friendPid = state.getLogicalPids().get(friendId);
            if(targetPid != friendPid) {
                TIntSet friendReplicaPids = state.getLogicalReplicaPids().get(friendId);
                if(!friendReplicaPids.contains(targetPid)) {
                    replicasToAdd.add(friendId);
                }
            }
        }

        return replicasToAdd;
    }

    static boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(Integer uid, int targetPid, State state) {
        Integer pid = state.getLogicalPids().get(uid);

        for(TIntIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            Integer friendBasePid = state.getLogicalPids().get(friendId);
            if (pid.equals(friendBasePid)) {
                return true;
            }
        }
        TIntSet replicas = state.getLogicalReplicaPids().get(uid);
        return replicas.size() <= state.getMinNumReplicas() && replicas.contains(targetPid);

    }

    static TIntSet findReplicasInMovingPartitionToDelete(int uid, int pid, TIntSet replicasToBeAdded, State state) {
        TIntSet replicasToDelete = new TIntHashSet();
        int minNumReplicas = state.getMinNumReplicas();

 outer: for(TIntIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            int friendPid = state.getLogicalPids().get(friendId);
            if (friendPid != pid) {
                int numReplicas = state.getLogicalReplicaPids().get(friendId).size() + (replicasToBeAdded.contains(friendId) ? 1 : 0);
                if(numReplicas <= minNumReplicas) {
                    continue;
                }
                for(TIntIterator iter2 = state.getFriendships().get(friendId).iterator(); iter2.hasNext(); ) {
                    int friendOfFriendId = iter2.next();
                    if (friendOfFriendId == uid) {
                        continue;
                    }

                    Integer friendOfFriendPid = state.getLogicalPids().get(friendOfFriendId);
                    if (friendOfFriendPid == pid) {
                        continue outer;
                    }
                }

                replicasToDelete.add(friendId);
            }
        }

        return replicasToDelete;
    }

    static boolean shouldDeleteReplicaInTargetPartition(Integer uid, Integer targetPid, State state) {
        if(state.getLogicalReplicaPids().get(uid).contains(targetPid)) {
            boolean addReplicaInCurrentPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, targetPid, state);
            int numReplicas = state.getLogicalReplicaPids().get(uid).size() + (addReplicaInCurrentPartition ? 1 : 0);
            return numReplicas > state.getMinNumReplicas();
        }
        return false;
    }

    static class SwapChanges {
        private Integer pid1;
        private Integer pid2;
        private TIntSet addToP1;
        private TIntSet addToP2;
        private TIntSet removeToP1;
        private TIntSet removeToP2;

        public Integer getPid1() {
            return pid1;
        }

        public void setPid1(Integer pid1) {
            this.pid1 = pid1;
        }

        public Integer getPid2() {
            return pid2;
        }

        public void setPid2(Integer pid2) {
            this.pid2 = pid2;
        }

        public TIntSet getAddToP1() {
            return addToP1;
        }

        public void setAddToP1(TIntSet addToP1) {
            this.addToP1 = addToP1;
        }

        public TIntSet getAddToP2() {
            return addToP2;
        }

        public void setAddToP2(TIntSet addToP2) {
            this.addToP2 = addToP2;
        }

        public TIntSet getRemoveFromP1() {
            return removeToP1;
        }

        public void setRemoveFromP1(TIntSet removeToP1) {
            this.removeToP1 = removeToP1;
        }

        public TIntSet getRemoveFromP2() {
            return removeToP2;
        }

        public void setRemoveFromP2(TIntSet removeToP2) {
            this.removeToP2 = removeToP2;
        }
    }

    static class State {
        private final int minNumReplicas;
        private final float alpha;
        private final TIntObjectMap<TIntSet> friendships;

        private TIntIntMap logicalPids;
        private TIntObjectMap<TIntSet> logicalReplicaPids;
        private TIntObjectMap<TIntSet> logicalReplicaPartitions;

        public State(int minNumReplicas, float alpha, float initialT, float deltaT, int k, TIntObjectMap<TIntSet> friendships) {
            this.minNumReplicas = minNumReplicas;
            this.alpha = alpha;
            this.friendships = friendships;
        }

        public int getMinNumReplicas() {
            return minNumReplicas;
        }

        public float getAlpha() {
            return alpha;
        }

        public TIntObjectMap<TIntSet> getFriendships() {
            return friendships;
        }

        public TIntIntMap getLogicalPids() {
            return logicalPids;
        }

        public void setLogicalPids(TIntIntMap logicalPids) {
            this.logicalPids = logicalPids;
        }

        public TIntObjectMap<TIntSet> getLogicalReplicaPids() {
            return logicalReplicaPids;
        }

        public void setLogicalReplicaPids(TIntObjectMap<TIntSet> logicalReplicaPids) {
            this.logicalReplicaPids = logicalReplicaPids;
        }

        public TIntObjectMap<TIntSet> getLogicalReplicaPartitions() {
            return logicalReplicaPartitions;
        }

        public void setLogicalReplicaPartitions(TIntObjectMap<TIntSet> logicalReplicaPartitions) {
            this.logicalReplicaPartitions = logicalReplicaPartitions;
        }
    }

}
