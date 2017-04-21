package io.vntr.j2;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class J2Repartitioner {
    private J2Manager manager;
    private float alpha;
    private float initialT;
    private float deltaT;
    private int k;

    public J2Repartitioner(J2Manager manager, float alpha, float initialT, float deltaT, int k) {
        this.manager = manager;
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
    }

    public int repartition(int numRestarts) {
        int bestEdgeCut = manager.getEdgeCut();
        Map<Integer, Integer> bestLogicalPids = null;

        int logicalMigrationCount = 0;
        for (int i = 0; i < numRestarts; i++) {
            State state = initState();
            state.setLogicalPids(getRandomLogicalPids(manager.getUserIds(), manager.getAllPartitionIds()));

            for(float t = initialT; t >= 1; t -= deltaT) {
                List<Integer> randomUserList = new LinkedList<>(manager.getUserIds());
                Collections.shuffle(randomUserList);
                for(Integer uid : randomUserList) {
                    Integer realPid = manager.getUser(uid).getPid();

                    Integer partnerId = findPartner(uid, sample(k, manager.getPartition(realPid)), t, state);
                    if(partnerId == null) {
                        partnerId = findPartner(uid, sample(k, manager.getUserIds()), t, state);
                    }
                    if(partnerId != null) {
                        logicalSwap(uid, partnerId, state);
                        logicalMigrationCount += 2;
                    }
                }
            }

            int edgeCut = getLogicalEdgeCut(state);
            if(edgeCut < bestEdgeCut) {
                bestEdgeCut = edgeCut;
                bestLogicalPids = new HashMap<>(state.getLogicalPids());
            }
        }

        if(bestLogicalPids != null) {
            physicallyMigrate(bestLogicalPids);
        }

        return logicalMigrationCount;
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

    State initState() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : manager.getUserIds()) {
            friendships.put(uid, new HashSet<>(manager.getFriendships().get(uid)));
        }
        return new State(alpha, initialT, deltaT, k, friendships);
    }

    void physicallyMigrate(Map<Integer, Integer> logicalPids) {
        for(Integer uid : logicalPids.keySet()) {
            J2User user = manager.getUser(uid);
            Integer newPid = logicalPids.get(uid);
            if(!user.getPid().equals(newPid)) {
                manager.moveUser(uid, newPid, false);
            }
        }
    }

    static int getLogicalEdgeCut(State state) {
        Map<Integer, Integer> uidToPidMap = state.getLogicalPids();
        int count = 0;
        for(Integer uid : uidToPidMap.keySet()) {
            Integer pid = uidToPidMap.get(uid);

            for(int friendId : state.getFriendships().get(uid)) {
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
