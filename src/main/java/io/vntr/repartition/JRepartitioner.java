package io.vntr.repartition;

import com.google.common.collect.Sets;

import java.util.*;

import static io.vntr.utils.Utils.getUToMasterMap;
import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class JRepartitioner {

    public static NoRepResults repartition(float alpha, float initialT, float deltaT, int k, int numRestarts, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, boolean incremental) {
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        int bestEdgeCut = getEdgeCut(uidToPidMap, friendships);
        Map<Integer, Integer> bestLogicalPids = null;

        int logicalMigrationCount = 0;
        for (int i = 0; i < numRestarts; i++) {
            State state = initState(alpha, friendships);
            Map<Integer, Set<Integer>> logicalPartitions = incremental ? partitions : getRandomLogicalPartitions(friendships.keySet(), partitions.keySet());
            state.setLogicalPids(getUToMasterMap(logicalPartitions));
            state.initUidToPidToFriendCount(logicalPartitions);

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

        return new NoRepResults(bestLogicalPids, logicalMigrationCount);
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

//            int[] myCounts = howManyFriendsHaveLogicalPartitions(uid, new int[]{logicalPid, theirLogicalPid}, state);
//            int[] theirCounts = howManyFriendsHaveLogicalPartitions(partnerId, new int[]{logicalPid, theirLogicalPid}, state);
            int myNeighborsOnMine      = state.getUidToPidToFriendCounts().get(uid).get(logicalPid);
            int myNeighborsOnTheirs    = state.getUidToPidToFriendCounts().get(uid).get(theirLogicalPid);
            int theirNeighborsOnMine   = state.getUidToPidToFriendCounts().get(partnerId).get(logicalPid);
            int theirNeighborsOnTheirs = state.getUidToPidToFriendCounts().get(partnerId).get(theirLogicalPid);

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

        for(int friendId : state.getFriendships().get(uid1)) {
            Map<Integer, Integer> counts = state.getUidToPidToFriendCounts().get(friendId);
            counts.put(pid1, counts.get(pid1) - 1);
            counts.put(pid2, counts.get(pid2) + 1);
        }
        for(int friendId : state.getFriendships().get(uid2)) {
            Map<Integer, Integer> counts = state.getUidToPidToFriendCounts().get(friendId);
            counts.put(pid2, counts.get(pid2) - 1);
            counts.put(pid1, counts.get(pid1) + 1);
        }

    }

    static State initState(float alpha, Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> friendshipsCopy = new HashMap<>();
        for(Integer uid : friendships.keySet()) {
            friendshipsCopy.put(uid, new HashSet<>(friendships.get(uid)));
        }
        return new State(alpha, friendshipsCopy);
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

    static Map<Integer, Set<Integer>> getRandomLogicalPartitions(Set<Integer> uids, Set<Integer> pids) {
        List<Integer> pidList = Arrays.asList(getPidsToAssign(uids.size(), pids));
        Collections.shuffle(pidList);

        Map<Integer, Set<Integer>> logicalPartitions = new HashMap<>();
        for(int pid : pids) {
            logicalPartitions.put(pid, new HashSet<Integer>());
        }

        int index = 0;
        for(Integer uid: uids) {
            logicalPartitions.get(pidList.get(index)).add(uid);
            index++;
        }

        return logicalPartitions;
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
        private final Map<Integer, Set<Integer>> friendships;
        private Map<Integer, Map<Integer, Integer>> uidToPidToFriendCounts;

        private Map<Integer, Integer> logicalPids;

        public State(float alpha, Map<Integer, Set<Integer>> friendships) {
            this.alpha = alpha;
            this.friendships = friendships;
        }

        public float getAlpha() {
            return alpha;
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

        public Map<Integer, Map<Integer, Integer>> getUidToPidToFriendCounts() {
            return uidToPidToFriendCounts;
        }

        public void initUidToPidToFriendCount(Map<Integer, Set<Integer>> partitions) {
            uidToPidToFriendCounts = new HashMap<>();
            for(int uid : friendships.keySet()) {
                Map<Integer, Integer> counts = new HashMap<>();
                for(int pid : partitions.keySet()) {
                    counts.put(pid, Sets.intersection(partitions.get(pid), friendships.get(uid)).size());
                }
                uidToPidToFriendCounts.put(uid, counts);
            }
        }
    }

}
