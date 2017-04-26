package io.vntr.repartition;

/**
 * Created by robertlindquist on 4/24/17.
 */
import java.util.*;

import static io.vntr.utils.Utils.*;
import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 4/15/17.
 */
public class SpJ2Repartitioner {

    public static Results repartition(int minNumReplicas, float alpha, float initialT, float deltaT, int k, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        int numRestarts = 1;
        int moves = 0;

        int bestNumReplicas = getLogicalReplicationCount(replicas);

        Map<Integer, Integer> bestLogicalPids = new HashMap<>();
        Map<Integer, Set<Integer>> bestLogicalReplicaPids = new HashMap<>();
        for(int i=0; i<numRestarts; i++) {
            State state = getState(minNumReplicas, alpha, initialT, deltaT, k, friendships, partitions, replicas);

            for(float t = initialT; t >= 1; t -= deltaT) {
                List<Integer> randomUserList = new LinkedList<>(friendships.keySet());
                Collections.shuffle(randomUserList);
                for (Integer uid : randomUserList) {
                    Set<Integer> swapCandidates = getKDistinctValuesFromList(k, friendships.keySet());

                    Integer partnerId = findPartner(uid, swapCandidates, t, state);
                    if(partnerId != null) {
                        swap(uid, partnerId, state);
                        moves += 2;
                    }
                }
            }

            int numReplicas = getLogicalReplicationCount(state.getLogicalReplicaPartitions());
            if(numReplicas < bestNumReplicas) {
                bestNumReplicas = numReplicas;
                bestLogicalPids = new HashMap<>(state.getLogicalPids());
                bestLogicalReplicaPids = new HashMap<>(state.getLogicalReplicaPids());
            }
        }

        Results results = new Results(moves, bestLogicalPids, bestLogicalReplicaPids);
        return results;
    }

    static Integer findPartner(Integer uid, Set<Integer> candidateIds, float t, State state) {
        Integer bestPartnerId = null;
        float bestScore = Float.MAX_VALUE;

        Integer logicalPid = state.getLogicalPids().get(uid);
        for(Integer partnerId : candidateIds) {
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

    static Integer getLogicalReplicationCount(Map<Integer, Set<Integer>> replicas) {
        int count = 0;
        for(Set<Integer> reps : replicas.values()) {
            count += reps.size();
        }
        return count;
    }

    static State getState(int minNumReplicas, float alpha, float initialT, float deltaT, int k, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        State state = new State(minNumReplicas, alpha, initialT, deltaT, k, friendships);

        state.setLogicalPids(getUToMasterMap(partitions));
        state.setLogicalReplicaPids(getUToReplicasMap(replicas, friendships.keySet()));

        Map<Integer, Set<Integer>> logicalReplicaPartitions = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            logicalReplicaPartitions.put(pid, new HashSet<>(replicas.get(pid)));
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

        for(Integer uidToAddToP1 : swapChanges.getAddToP1()) {
            state.getLogicalReplicaPids().get(uidToAddToP1).add(pid1);
        }

        for(Integer uidToAddToP2 : swapChanges.getAddToP2()) {
            state.getLogicalReplicaPids().get(uidToAddToP2).add(pid2);
        }

        for(Integer uidToRemoveFromP1 : swapChanges.getRemoveFromP1()) {
            state.getLogicalReplicaPids().get(uidToRemoveFromP1).remove(pid1);
        }

        for(Integer uidToRemoveFromP2 : swapChanges.getRemoveFromP2()) {
            state.getLogicalReplicaPids().get(uidToRemoveFromP2).remove(pid2);
        }

        state.getLogicalReplicaPartitions().get(pid1).addAll(swapChanges.getAddToP1());
        state.getLogicalReplicaPartitions().get(pid2).addAll(swapChanges.getAddToP2());

        state.getLogicalReplicaPartitions().get(pid1).removeAll(swapChanges.getRemoveFromP1());
        state.getLogicalReplicaPartitions().get(pid2).removeAll(swapChanges.getRemoveFromP2());
    }

    static SwapChanges getSwapChanges(Integer uid1, Integer uid2, State state) {
        Set<Integer> u1Friends = state.getFriendships().get(uid1);
        Set<Integer> u2Friends = state.getFriendships().get(uid2);

        Integer pid1 = state.getLogicalPids().get(uid1);
        Integer pid2 = state.getLogicalPids().get(uid2);

        boolean u1AndU2AreFriends = u1Friends.contains(uid2);

        Set<Integer> mutualFriends = new HashSet<>(u1Friends);
        mutualFriends.retainAll(u2Friends);

        SwapChanges swapChanges = new SwapChanges();
        swapChanges.setPid1(pid1);
        swapChanges.setPid2(pid2);

        Set<Integer> addToP1 = findReplicasToAddToTargetPartition(uid2, pid1, state);
        if(u1AndU2AreFriends || shouldWeAddAReplicaOfMovingUserInMovingPartition(uid1, pid2, state)) {
            addToP1.add(uid1);
        }
        swapChanges.setAddToP1(addToP1);

        Set<Integer> addToP2 = findReplicasToAddToTargetPartition(uid1, pid2, state);
        if(u1AndU2AreFriends || shouldWeAddAReplicaOfMovingUserInMovingPartition(uid2, pid1, state)) {
            addToP2.add(uid2);
        }
        swapChanges.setAddToP2(addToP2);

        Set<Integer> removeFromP1 = findReplicasInMovingPartitionToDelete(uid1, pid1, addToP1, state);
        if(shouldDeleteReplicaInTargetPartition(uid2, pid1, state)) {
            removeFromP1.add(uid2);
        }
        removeFromP1.removeAll(mutualFriends);
        swapChanges.setRemoveFromP1(removeFromP1);

        Set<Integer> removeFromP2 = findReplicasInMovingPartitionToDelete(uid2, pid2, addToP2, state);
        if(shouldDeleteReplicaInTargetPartition(uid1, pid2, state)) {
            removeFromP2.add(uid1);
        }
        removeFromP2.removeAll(mutualFriends);
        swapChanges.setRemoveFromP2(removeFromP2);

        return swapChanges;
    }

    static Set<Integer> findReplicasToAddToTargetPartition(Integer uid, Integer targetPartitionId, State state) {
        Set<Integer> replicasToAdd = new HashSet<>();
        for (Integer friendId : state.getFriendships().get(uid)) {
            int friendPid = state.getLogicalPids().get(friendId);
            if(targetPartitionId != friendPid) {
                Set<Integer> friendReplicaPids = state.getLogicalReplicaPids().get(friendId);
                if(!friendReplicaPids.contains(targetPartitionId)) {
                    replicasToAdd.add(friendId);
                }
            }
        }

        return replicasToAdd;
    }

    static boolean shouldWeAddAReplicaOfMovingUserInMovingPartition(Integer uid, int targetPid, State state) {
        Integer pid = state.getLogicalPids().get(uid);
        for (Integer friendId : state.getFriendships().get(uid)) {
            Integer friendMasterPartitionId = state.getLogicalPids().get(friendId);
            if (pid.equals(friendMasterPartitionId)) {
                return true;
            }
        }
        Set<Integer> replicas = state.getLogicalReplicaPids().get(uid);
        return replicas.size() <= state.getMinNumReplicas() && replicas.contains(targetPid);

    }

    static Set<Integer> findReplicasInMovingPartitionToDelete(int uid, int pid, Set<Integer> replicasToBeAdded, State state) {
        Set<Integer> replicasToDelete = new HashSet<>();
        int minNumReplicas = state.getMinNumReplicas();

        outer:  for(Integer friendId : state.getFriendships().get(uid)) {
            int friendPid = state.getLogicalPids().get(friendId);
            if (friendPid != pid) {
                int numReplicas = state.getLogicalReplicaPids().get(friendId).size() + (replicasToBeAdded.contains(friendId) ? 1 : 0);
                if(numReplicas <= minNumReplicas) {
                    continue;
                }
                for (Integer friendOfFriendId : state.getFriendships().get(friendId)) {
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
        private Set<Integer> addToP1;
        private Set<Integer> addToP2;
        private Set<Integer> removeToP1;
        private Set<Integer> removeToP2;

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

        public Set<Integer> getAddToP1() {
            return addToP1;
        }

        public void setAddToP1(Set<Integer> addToP1) {
            this.addToP1 = addToP1;
        }

        public Set<Integer> getAddToP2() {
            return addToP2;
        }

        public void setAddToP2(Set<Integer> addToP2) {
            this.addToP2 = addToP2;
        }

        public Set<Integer> getRemoveFromP1() {
            return removeToP1;
        }

        public void setRemoveFromP1(Set<Integer> removeToP1) {
            this.removeToP1 = removeToP1;
        }

        public Set<Integer> getRemoveFromP2() {
            return removeToP2;
        }

        public void setRemoveFromP2(Set<Integer> removeToP2) {
            this.removeToP2 = removeToP2;
        }
    }

    static class State {
        private final int minNumReplicas;
        private final float alpha;
        private final float initialT;
        private final float deltaT;
        private final int k;
        private final Map<Integer, Set<Integer>> friendships;

        private Map<Integer, Integer> logicalPids;
        private Map<Integer, Set<Integer>> logicalReplicaPids;
        private Map<Integer, Set<Integer>> logicalReplicaPartitions;

        public State(int minNumReplicas, float alpha, float initialT, float deltaT, int k, Map<Integer, Set<Integer>> friendships) {
            this.minNumReplicas = minNumReplicas;
            this.alpha = alpha;
            this.initialT = initialT;
            this.deltaT = deltaT;
            this.k = k;
            this.friendships = friendships;
        }

        public int getMinNumReplicas() {
            return minNumReplicas;
        }

        public float getAlpha() {
            return alpha;
        }

        public float getInitialT() {
            return initialT;
        }

        public float getDeltaT() {
            return deltaT;
        }

        public int getK() {
            return k;
        }

        public Map<Integer, Set<Integer>> getFriendships() {
            return friendships;
        }

        public Map<Integer, Integer> getLogicalPids() {
            return logicalPids;
        }

        public void setLogicalPids(Map<Integer, Integer> logicalPids) {
            this.logicalPids = logicalPids;
        }

        public Map<Integer, Set<Integer>> getLogicalReplicaPids() {
            return logicalReplicaPids;
        }

        public void setLogicalReplicaPids(Map<Integer, Set<Integer>> logicalReplicaPids) {
            this.logicalReplicaPids = logicalReplicaPids;
        }

        public Map<Integer, Set<Integer>> getLogicalReplicaPartitions() {
            return logicalReplicaPartitions;
        }

        public void setLogicalReplicaPartitions(Map<Integer, Set<Integer>> logicalReplicaPartitions) {
            this.logicalReplicaPartitions = logicalReplicaPartitions;
        }
    }

    public static class Results {
        private final int moves;
        private final Map<Integer, Integer> newPids;
        private final Map<Integer, Set<Integer>> newReplicaPids;

        public Results(int moves, Map<Integer, Integer> newPids, Map<Integer, Set<Integer>> newReplicaPids) {
            this.moves = moves;
            this.newPids = newPids;
            this.newReplicaPids = newReplicaPids;
        }

        public int getMoves() {
            return moves;
        }

        public Map<Integer, Integer> getNewPids() {
            return newPids;
        }

        public Map<Integer, Set<Integer>> getNewReplicaPids() {
            return newReplicaPids;
        }
    }
}
