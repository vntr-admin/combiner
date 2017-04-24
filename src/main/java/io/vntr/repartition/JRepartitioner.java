package io.vntr.repartition;

import java.util.*;

import static io.vntr.Utils.getUToMasterMap;
import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class JRepartitioner {

    public static Results repartition(float alpha, float initialT, float deltaT, int k, int numRestarts, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, boolean incremental) {
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        int bestEdgeCut = getEdgeCut(uidToPidMap, friendships);
        Map<Integer, Integer> bestLogicalPids = null;

        int logicalMigrationCount = 0;
        for (int i = 0; i < numRestarts; i++) {
            State state = initState(alpha, initialT, deltaT, k, friendships);
            state.setLogicalPids(incremental ? new HashMap<>(uidToPidMap) : getRandomLogicalPids(friendships.keySet(), partitions.keySet()));

            for(float t = initialT; t >= 1; t -= deltaT) {
                List<Integer> randomUserList = new LinkedList<>(friendships.keySet());
                Collections.shuffle(randomUserList);
                for(Integer uid : randomUserList) {
                    Integer partnerId = null;
                    if(!incremental) {
                        partnerId = findPartner(uid, sample(k, partitions.get(uidToPidMap.get(uid))), t, state);
                    }
                    if(partnerId == null) {
                        partnerId = findPartner(uid, sample(k, friendships.keySet()), t, state);
                    }
                    if(partnerId != null) {
                        logicalSwap(uid, partnerId, state);
                        logicalMigrationCount += 2;
                    }
                }
            }

            int edgeCut = getEdgeCut(state.getLogicalPids(), state.getFriendships());
            if(edgeCut < bestEdgeCut) {
                bestEdgeCut = edgeCut;
                bestLogicalPids = new HashMap<>(state.getLogicalPids());
            }
        }

        return new Results(bestLogicalPids, logicalMigrationCount);
    }

    static Integer findPartner(Integer uid, Set<Integer> candidates, float t, State state) {
        Integer bestPartner = null;
        float bestScore = 0f;

        Integer logicalPid = state.getLogicalPids().get(uid);

        for(Integer partnerId : candidates) {
            Integer theirLogicalPid = state.getLogicalPids().get(partnerId);
            if(theirLogicalPid.equals(logicalPid)) {
                continue;
            }

            int[] myCounts = howManyFriendsHaveLogicalPartitions(uid, new int[]{logicalPid, theirLogicalPid}, state);
            int[] theirCounts = howManyFriendsHaveLogicalPartitions(partnerId, new int[]{logicalPid, theirLogicalPid}, state);
            int myNeighborsOnMine      = myCounts[0];
            int myNeighborsOnTheirs    = myCounts[1];
            int theirNeighborsOnMine   = theirCounts[0];
            int theirNeighborsOnTheirs = theirCounts[1];

            float oldScore = (float) (Math.pow(myNeighborsOnMine, state.getAlpha()) + Math.pow(theirNeighborsOnTheirs, state.getAlpha()));
            float newScore = (float) (Math.pow(myNeighborsOnTheirs, state.getAlpha()) + Math.pow(theirNeighborsOnMine, state.getAlpha()));

            if(newScore > bestScore && (newScore * t) > oldScore) {
                bestPartner = partnerId;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    static int[] howManyFriendsHaveLogicalPartitions(int uid, int[] pids, State state) {
        int[] counts = new int[pids.length];
        for(Integer friendId : state.getFriendships().get(uid)) {
            int friendPid = state.getLogicalPids().get(friendId);
            for(int i=0; i<pids.length; i++) {
                if(pids[i] == friendPid) {
                    counts[i]++;
                }
            }
        }
        return counts;
    }

    static Set<Integer> sample(int n, Set<Integer> uids) {
        return uids.size() > n ? getKDistinctValuesFromList(n, uids) : new HashSet<>(uids);
    }

    static void logicalSwap(Integer uid1, Integer uid2, State state) {
        Integer pid1 = state.getLogicalPids().get(uid1);
        Integer pid2 = state.getLogicalPids().get(uid2);

        state.getLogicalPids().put(uid1, pid2);
        state.getLogicalPids().put(uid2, pid1);
    }

    static State initState(float alpha, float initialT, float deltaT, int k, Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> friendshipsCopy = new HashMap<>();
        for(Integer uid : friendships.keySet()) {
            friendshipsCopy.put(uid, new HashSet<>(friendships.get(uid)));
        }
        return new State(alpha, initialT, deltaT, k, friendshipsCopy);
    }

    static int getEdgeCut(Map<Integer, Integer> uidToPidMap, Map<Integer, Set<Integer>> friendships) {
        int count = 0;
        for(Integer uid : uidToPidMap.keySet()) {
            Integer pid = uidToPidMap.get(uid);

            for(int friendId : friendships.get(uid)) {
                Integer friendPid = uidToPidMap.get(friendId);
                if(pid < friendPid) {
                    count++;
                }
            }
        }
        return count;
    }

    static Map<Integer, Integer> getRandomLogicalPids(Set<Integer> uids, Set<Integer> pids) {
        List<Integer> pidList = Arrays.asList(getPidsToAssign(uids.size(), pids));
        Collections.shuffle(pidList);

        Map<Integer, Integer> logicalPids = new HashMap<>();
        int index = 0;
        for(Integer uid: uids) {
            logicalPids.put(uid, pidList.get(index));
            index++;
        }

        return logicalPids;
    }

    static Integer[] getPidsToAssign(int numUsers, Set<Integer> pids) {
        //Fill array with pids such that:
        //(1) array.length = numUsers
        //(2) no pid occurs more than ceiling(numUsers/numPartitions) times
        //Note: The order is unimportant, since we'll shuffle it later
        Integer[] replicatedPids = new Integer[numUsers];

        //Step 1: fill the first numPartitions * floor(numUsers/numPartitions) elements
        //This is easy, because we can just put floor(numUsers/numPartitions) copies of each pid
        int floorUsersPerPartition = numUsers / pids.size();

        int index = 0;
        for(int i : pids) {
            Arrays.fill(replicatedPids, index, index + floorUsersPerPartition, i);
            index += floorUsersPerPartition;
        }

        //Step 2: fill the remainder (if any) with randomly-selected pids (no more than once each)
        List<Integer> remainingPids = new ArrayList<>(pids);
        Collections.shuffle(remainingPids);
        Iterator<Integer> pidIter = remainingPids.iterator();
        while(index < replicatedPids.length) {
            replicatedPids[index] = pidIter.next();
            index++;
        }

        return replicatedPids;
    }

    static class State {
        private final float alpha;
        private final float initialT;
        private final float deltaT;
        private final int k;
        private final Map<Integer, Set<Integer>> friendships;

        private Map<Integer, Integer> logicalPids;

        public State(float alpha, float initialT, float deltaT, int k, Map<Integer, Set<Integer>> friendships) {
            this.alpha = alpha;
            this.initialT = initialT;
            this.deltaT = deltaT;
            this.k = k;
            this.friendships = friendships;
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
    }

}
