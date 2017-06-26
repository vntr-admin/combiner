package io.vntr.repartition;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/15/17.
 */
public class SpajaRepartitioner {

    public static RepResults repartition(short minNumReplicas, float alpha, float initialT, float deltaT, short k, TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas) {
        int numRestarts = 1;
        int moves = 0;

        int bestNumReplicas = getLogicalReplicationCount(replicas);

        TShortShortMap bestLogicalPids = new TShortShortHashMap(partitions.size()+1);
        TShortObjectMap<TShortSet> bestLogicalReplicaPids = new TShortObjectHashMap<>(partitions.size()+1);
        for(int i=0; i<numRestarts; i++) {
            State state = getState(minNumReplicas, alpha, initialT, deltaT, k, friendships, partitions, replicas);

            for(float t = initialT; t >= 1; t -= deltaT) {
                short[] randomUserIdArray = new short[friendships.size()];
                System.arraycopy(friendships.keys(), 0, randomUserIdArray, 0, friendships.size());
                shuffle(randomUserIdArray);
                for (short uid : randomUserIdArray) {
                    TShortSet swapCandidates = getKDistinctValuesFromArray(k, friendships.keys());

                    Short partnerId = findPartner(uid, swapCandidates, t, state);
                    if(partnerId != null) {
                        swap(uid, partnerId, state);
                        moves += 2;
                    }
                }
            }

            int numReplicas = getLogicalReplicationCount(state.getLogicalReplicaPartitions());
            if(numReplicas < bestNumReplicas) {
                bestNumReplicas = numReplicas;
                bestLogicalPids = new TShortShortHashMap(state.getLogicalPids());
                bestLogicalReplicaPids = new TShortObjectHashMap<>(state.getLogicalReplicaPids());
            }
        }

        RepResults repResults = new RepResults(moves, bestLogicalPids, bestLogicalReplicaPids);
        return repResults;
    }

    static Short findPartner(short uid, TShortSet candidateIds, float t, State state) {
        Short bestPartnerId = null;
        float bestScore = Float.MAX_VALUE;

        short logicalPid = state.getLogicalPids().get(uid);
        for(TShortIterator iter = candidateIds.iterator(); iter.hasNext(); ) {
            short partnerId = iter.next();
            short partnerLogicalPid = state.getLogicalPids().get(partnerId);
            if(logicalPid == partnerLogicalPid) {
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

    static Integer getLogicalReplicationCount(TShortObjectMap<TShortSet> replicas) {
        int count = 0;
        for(Object reps : replicas.values()) {
            count += ((TShortSet) reps).size();
        }
        return count;
    }

    static State getState(short minNumReplicas, float alpha, float initialT, float deltaT, short k, TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas) {
        State state = new State(minNumReplicas, alpha, initialT, deltaT, k, friendships);

        state.setLogicalPids(getUToMasterMap(partitions));
        state.setLogicalReplicaPids(getUToReplicasMap(replicas, friendships.keySet()));

        TShortObjectMap<TShortSet> logicalReplicaPartitions = new TShortObjectHashMap<>(partitions.size()+1);
        for(short pid : partitions.keys()) {
            logicalReplicaPartitions.put(pid, new TShortHashSet(replicas.get(pid)));
        }
        state.setLogicalReplicaPartitions(logicalReplicaPartitions);

        return state;
    }

    static float calcScore(int replicasInP1, int replicasInP2, float alpha) {
        return (float)(Math.pow(replicasInP1, alpha) + Math.pow(replicasInP2, alpha));
    }

    static void swap(short uid1, short uid2, State state) {
        SwapChanges swapChanges = getSwapChanges(uid1, uid2, state);

        short pid1 = state.getLogicalPids().get(uid1);
        short pid2 = state.getLogicalPids().get(uid2);

        state.getLogicalPids().put(uid1, pid2);
        state.getLogicalPids().put(uid2, pid1);

        for(TShortIterator iter = swapChanges.getAddToP1().iterator(); iter.hasNext(); ) {
            state.getLogicalReplicaPids().get(iter.next()).add(pid1);
        }
        for(TShortIterator iter = swapChanges.getAddToP2().iterator(); iter.hasNext(); ) {
            state.getLogicalReplicaPids().get(iter.next()).add(pid2);
        }
        for(TShortIterator iter = swapChanges.getRemoveFromP1().iterator(); iter.hasNext(); ) {
            state.getLogicalReplicaPids().get(iter.next()).remove(pid1);
        }
        for(TShortIterator iter = swapChanges.getRemoveFromP2().iterator(); iter.hasNext(); ) {
            state.getLogicalReplicaPids().get(iter.next()).remove(pid2);
        }

        state.getLogicalReplicaPartitions().get(pid1).addAll(swapChanges.getAddToP1());
        state.getLogicalReplicaPartitions().get(pid2).addAll(swapChanges.getAddToP2());

        state.getLogicalReplicaPartitions().get(pid1).removeAll(swapChanges.getRemoveFromP1());
        state.getLogicalReplicaPartitions().get(pid2).removeAll(swapChanges.getRemoveFromP2());
    }

    static SwapChanges getSwapChanges(short uid1, short uid2, State state) {
        TShortSet u1Friends = state.getFriendships().get(uid1);
        TShortSet u2Friends = state.getFriendships().get(uid2);

        short pid1 = state.getLogicalPids().get(uid1);
        short pid2 = state.getLogicalPids().get(uid2);

        boolean u1AndU2AreFriends = u1Friends.contains(uid2);

        TShortSet mutualFriends = new TShortHashSet(u1Friends);
        mutualFriends.retainAll(u2Friends);

        SwapChanges swapChanges = new SwapChanges();
        swapChanges.setPid1(pid1);
        swapChanges.setPid2(pid2);

        TShortSet addToP1 = findReplicasToAddToTargetPartition(uid2, pid1, state);
        if(u1AndU2AreFriends || shouldWeAddAReplicaOfMovingUserInMovingPartition(uid1, pid2, state)) {
            addToP1.add(uid1);
        }
        swapChanges.setAddToP1(addToP1);

        TShortSet addToP2 = findReplicasToAddToTargetPartition(uid1, pid2, state);
        if(u1AndU2AreFriends || shouldWeAddAReplicaOfMovingUserInMovingPartition(uid2, pid1, state)) {
            addToP2.add(uid2);
        }
        swapChanges.setAddToP2(addToP2);

        TShortSet removeFromP1 = findReplicasInMovingPartitionToDelete(uid1, pid1, addToP1, state);
        if(shouldDeleteReplicaInTargetPartition(uid2, pid1, state)) {
            removeFromP1.add(uid2);
        }
        removeFromP1.removeAll(mutualFriends);
        swapChanges.setRemoveFromP1(removeFromP1);

        TShortSet removeFromP2 = findReplicasInMovingPartitionToDelete(uid2, pid2, addToP2, state);
        if(shouldDeleteReplicaInTargetPartition(uid1, pid2, state)) {
            removeFromP2.add(uid1);
        }
        removeFromP2.removeAll(mutualFriends);
        swapChanges.setRemoveFromP2(removeFromP2);

        return swapChanges;
    }

    static TShortSet findReplicasToAddToTargetPartition(short uid, short targetPid, State state) {
        TShortSet replicasToAdd = new TShortHashSet();
        for(TShortIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            short friendPid = state.getLogicalPids().get(friendId);
            if(targetPid != friendPid) {
                TShortSet friendReplicaPids = state.getLogicalReplicaPids().get(friendId);
                if(!friendReplicaPids.contains(targetPid)) {
                    replicasToAdd.add(friendId);
                }
            }
        }

        return replicasToAdd;
    }

    static boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(short uid, short targetPid, State state) {
        short pid = state.getLogicalPids().get(uid);

        for(TShortIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            short friendBasePid = state.getLogicalPids().get(friendId);
            if (pid == friendBasePid) {
                return true;
            }
        }
        TShortSet replicas = state.getLogicalReplicaPids().get(uid);
        return replicas.size() <= state.getMinNumReplicas() && replicas.contains(targetPid);

    }

    static TShortSet findReplicasInMovingPartitionToDelete(short uid, short pid, TShortSet replicasToBeAdded, State state) {
        TShortSet replicasToDelete = new TShortHashSet();
        int minNumReplicas = state.getMinNumReplicas();

 outer: for(TShortIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            short friendPid = state.getLogicalPids().get(friendId);
            if (friendPid != pid) {
                int numReplicas = state.getLogicalReplicaPids().get(friendId).size() + (replicasToBeAdded.contains(friendId) ? 1 : 0);
                if(numReplicas <= minNumReplicas) {
                    continue;
                }
                for(TShortIterator iter2 = state.getFriendships().get(friendId).iterator(); iter2.hasNext(); ) {
                    short friendOfFriendId = iter2.next();
                    if (friendOfFriendId == uid) {
                        continue;
                    }

                    short friendOfFriendPid = state.getLogicalPids().get(friendOfFriendId);
                    if (friendOfFriendPid == pid) {
                        continue outer;
                    }
                }

                replicasToDelete.add(friendId);
            }
        }

        return replicasToDelete;
    }

    static boolean shouldDeleteReplicaInTargetPartition(short uid, short targetPid, State state) {
        if(state.getLogicalReplicaPids().get(uid).contains(targetPid)) {
            boolean addReplicaInCurrentPartition = shouldWeAddAReplicaOfMovingUserInMovingPartition(uid, targetPid, state);
            int numReplicas = state.getLogicalReplicaPids().get(uid).size() + (addReplicaInCurrentPartition ? 1 : 0);
            return numReplicas > state.getMinNumReplicas();
        }
        return false;
    }

    static class SwapChanges {
        private short pid1;
        private short pid2;
        private TShortSet addToP1;
        private TShortSet addToP2;
        private TShortSet removeToP1;
        private TShortSet removeToP2;

        public short getPid1() {
            return pid1;
        }

        public void setPid1(short pid1) {
            this.pid1 = pid1;
        }

        public short getPid2() {
            return pid2;
        }

        public void setPid2(short pid2) {
            this.pid2 = pid2;
        }

        public TShortSet getAddToP1() {
            return addToP1;
        }

        public void setAddToP1(TShortSet addToP1) {
            this.addToP1 = addToP1;
        }

        public TShortSet getAddToP2() {
            return addToP2;
        }

        public void setAddToP2(TShortSet addToP2) {
            this.addToP2 = addToP2;
        }

        public TShortSet getRemoveFromP1() {
            return removeToP1;
        }

        public void setRemoveFromP1(TShortSet removeToP1) {
            this.removeToP1 = removeToP1;
        }

        public TShortSet getRemoveFromP2() {
            return removeToP2;
        }

        public void setRemoveFromP2(TShortSet removeToP2) {
            this.removeToP2 = removeToP2;
        }
    }

    static class State {
        private final short minNumReplicas;
        private final float alpha;
        private final TShortObjectMap<TShortSet> friendships;

        private TShortShortMap logicalPids;
        private TShortObjectMap<TShortSet> logicalReplicaPids;
        private TShortObjectMap<TShortSet> logicalReplicaPartitions;

        public State(short minNumReplicas, float alpha, float initialT, float deltaT, short k, TShortObjectMap<TShortSet> friendships) {
            this.minNumReplicas = minNumReplicas;
            this.alpha = alpha;
            this.friendships = friendships;
        }

        public short getMinNumReplicas() {
            return minNumReplicas;
        }

        public float getAlpha() {
            return alpha;
        }

        public TShortObjectMap<TShortSet> getFriendships() {
            return friendships;
        }

        public TShortShortMap getLogicalPids() {
            return logicalPids;
        }

        public void setLogicalPids(TShortShortMap logicalPids) {
            this.logicalPids = logicalPids;
        }

        public TShortObjectMap<TShortSet> getLogicalReplicaPids() {
            return logicalReplicaPids;
        }

        public void setLogicalReplicaPids(TShortObjectMap<TShortSet> logicalReplicaPids) {
            this.logicalReplicaPids = logicalReplicaPids;
        }

        public TShortObjectMap<TShortSet> getLogicalReplicaPartitions() {
            return logicalReplicaPartitions;
        }

        public void setLogicalReplicaPartitions(TShortObjectMap<TShortSet> logicalReplicaPartitions) {
            this.logicalReplicaPartitions = logicalReplicaPartitions;
        }
    }

}
