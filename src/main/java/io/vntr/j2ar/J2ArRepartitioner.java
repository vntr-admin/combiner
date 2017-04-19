package io.vntr.j2ar;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 4/17/17.
 */
public class J2ArRepartitioner {
    private J2ArManager manager;
    private float alpha;
    private float initialT;
    private float deltaT;
    private int k;

    public J2ArRepartitioner(J2ArManager manager, float alpha, float initialT, float deltaT, int k) {
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
            state.setLogicalPids(getLogicalPids());

            for(float t = initialT; t >= 1; t -= deltaT) {
                List<Integer> randomUserList = new LinkedList<>(manager.getUserIds());
                Collections.shuffle(randomUserList);
                for(Integer uid : randomUserList) {
                    Integer partnerId = findPartner(uid, sample(k, manager.getUserIds()), t, state);
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

            int myNeighborsOnMine      = howManyFriendsHaveLogicalPartition(uid, logicalPid, state);
            int myNeighborsOnTheirs    = howManyFriendsHaveLogicalPartition(uid, theirLogicalPid, state);
            int theirNeighborsOnMine   = howManyFriendsHaveLogicalPartition(partnerId, logicalPid, state);
            int theirNeighborsOnTheirs = howManyFriendsHaveLogicalPartition(partnerId, theirLogicalPid, state);

            float oldScore = (float) (Math.pow(myNeighborsOnMine, state.getAlpha()) + Math.pow(theirNeighborsOnTheirs, state.getAlpha()));
            float newScore = (float) (Math.pow(myNeighborsOnTheirs, state.getAlpha()) + Math.pow(theirNeighborsOnMine, state.getAlpha()));

            if(newScore > bestScore && (newScore * t) > oldScore) {
                bestPartner = partnerId;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    static int howManyFriendsHaveLogicalPartition(Integer uid, Integer pid, State state) {
        int count = 0;
        for(Integer friendId : state.getFriendships().get(uid)) {
            if(state.getLogicalPids().get(friendId).equals(pid)) {
                count++;
            }
        }
        return count;
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
            J2ArUser user = manager.getUser(uid);
            Integer newPid = logicalPids.get(uid);
            if(!user.getPid().equals(newPid)) {
                manager.moveUser(uid, newPid, false);
            }
        }
    }

    static int getLogicalEdgeCut(State state) {
        Map<Integer, Integer> pids = state.getLogicalPids();
        int count = 0;
        for(Integer uid : pids.keySet()) {
            Integer pid = pids.get(uid);

            for(int friendId : state.getFriendships().get(uid)) {
                Integer friendPid = pids.get(friendId);
                if(pid < friendPid) {
                    count++;
                }
            }
        }
        return count;
    }

    Map<Integer, Integer> getLogicalPids() {
        Map<Integer, Integer> logicalPids = new HashMap<>();
        for(Integer uid : manager.getUserIds()) {
            logicalPids.put(uid, manager.getUser(uid).getPid());
        }
        return logicalPids;
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
