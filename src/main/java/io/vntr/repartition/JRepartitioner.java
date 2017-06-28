package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Arrays;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class JRepartitioner {

    public static NoRepResults repartition(float alpha, float initialT, float deltaT, int k, int numRestarts, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> friendships, boolean incremental, boolean earlyTermination) {
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        int bestEdgeCut = getEdgeCut(uidToPidMap, friendships);
        TIntIntMap bestLogicalPids = null;

        int logicalMigrationCount = 0;
        for (int i = 0; i < numRestarts; i++) {
            State state = initState(alpha, friendships);
            TIntObjectMap<TIntSet> logicalPartitions = incremental ? partitions : getRandomLogicalPartitions(friendships.keySet(), partitions.keySet());
            state.setLogicalPids(getUToMasterMap(logicalPartitions));
            state.initUidToPidToFriendCount(logicalPartitions);

            for(float t = initialT; t >= 1; t -= deltaT) {
                int logicalMigrationCountBefore = logicalMigrationCount;
                int[] randomUserArray = friendships.keys();
                shuffle(randomUserArray);
                for(Integer uid : randomUserArray) {
                    Integer partnerId = null;
                    if(!incremental) {
                        partnerId = findPartner(uid, sample(k, partitions.get(uidToPidMap.get(uid))), t, state);
                    }
                    if(partnerId == null) {
                        partnerId = findPartner(uid, sample(k, friendships.keySet()), t, state);
                    }
                    if(partnerId != null) {
                        boolean localSwap = uidToPidMap.get(uid) == uidToPidMap.get(partnerId);
                        logicalSwap(uid, partnerId, state);
                        if(!localSwap) {
                            logicalMigrationCount += 2;
                        }
                    }
                }
                if(earlyTermination && logicalMigrationCount == logicalMigrationCountBefore) {
                    break;
                }
            }

            int edgeCut = getEdgeCut(state.getLogicalPids(), state.getFriendships());
            if(edgeCut < bestEdgeCut) {
                bestEdgeCut = edgeCut;
                bestLogicalPids = new TIntIntHashMap(state.getLogicalPids());
            }
        }

        return new NoRepResults(bestLogicalPids, logicalMigrationCount);
    }

    static Integer findPartner(Integer uid, TIntSet candidates, float t, State state) {
        Integer bestPartner = null;
        float bestScore = 0f;

        Integer logicalPid = state.getLogicalPids().get(uid);

        for(TIntIterator iter = candidates.iterator(); iter.hasNext(); ) {
            int partnerId = iter.next();
            Integer theirLogicalPid = state.getLogicalPids().get(partnerId);
            if(theirLogicalPid.equals(logicalPid)) {
                continue;
            }

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
        for(TIntIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            int friendPid = state.getLogicalPids().get(friendId);
            for(int i=0; i<pids.length; i++) {
                if(pids[i] == friendPid) {
                    counts[i]++;
                }
            }
        }
        return counts;
    }

    static TIntSet sample(int n, TIntSet uids) {
        return uids.size() > n ? getKDistinctValuesFromArray(n, uids.toArray()) : new TIntHashSet(uids);
    }

    static void logicalSwap(Integer uid1, Integer uid2, State state) {
        Integer pid1 = state.getLogicalPids().get(uid1);
        Integer pid2 = state.getLogicalPids().get(uid2);

        state.getLogicalPids().put(uid1, pid2);
        state.getLogicalPids().put(uid2, pid1);

        for(TIntIterator iter = state.getFriendships().get(uid1).iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            TIntIntMap counts = state.getUidToPidToFriendCounts().get(friendId);
            counts.put(pid1, counts.get(pid1) - 1);
            counts.put(pid2, counts.get(pid2) + 1);
        }

        for(TIntIterator iter = state.getFriendships().get(uid1).iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            TIntIntMap counts = state.getUidToPidToFriendCounts().get(friendId);
            counts.put(pid2, counts.get(pid2) - 1);
            counts.put(pid1, counts.get(pid1) + 1);
        }

    }

    static State initState(float alpha, TIntObjectMap<TIntSet> friendships) {
        TIntObjectMap<TIntSet> friendshipsCopy = new TIntObjectHashMap<>(friendships.size()+1);
        for(Integer uid : friendships.keys()) {
            friendshipsCopy.put(uid, new TIntHashSet(friendships.get(uid)));
        }
        return new State(alpha, friendshipsCopy);
    }

    static int getEdgeCut(TIntIntMap uidToPidMap, TIntObjectMap<TIntSet> friendships) {
        int count = 0;
        for(Integer uid : uidToPidMap.keys()) {
            Integer pid = uidToPidMap.get(uid);

            for(TIntIterator iter = friendships.get(uid).iterator(); iter.hasNext(); ) {
                int friendId = iter.next();
                Integer friendPid = uidToPidMap.get(friendId);
                if(pid < friendPid) {
                    count++;
                }
            }
        }
        return count;
    }

    static TIntObjectMap<TIntSet> getRandomLogicalPartitions(TIntSet uids, TIntSet pids) {
        int[] pidArray = getPidsToAssign(uids.size(), pids);
        shuffle(pidArray);

        TIntObjectMap<TIntSet> logicalPartitions = new TIntObjectHashMap<>(pids.size()+1);
        for(TIntIterator iter = pids.iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            logicalPartitions.put(pid, new TIntHashSet());
        }

        int index = 0;
        for(TIntIterator iter = uids.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            logicalPartitions.get(pidArray[index]).add(uid);
            index++;
        }

        return logicalPartitions;
    }

    static int[] getPidsToAssign(int numUsers, TIntSet pids) {
        //Fill array with pids such that:
        //(1) array.length = numUsers
        //(2) no pid occurs more than ceiling(numUsers/numPartitions) times
        //Note: The order is unimportant, since we'll shuffle it later
        int[] replicatedPids = new int[numUsers];

        //Step 1: fill the first numPartitions * floor(numUsers/numPartitions) elements
        //This is easy, because we can just put floor(numUsers/numPartitions) copies of each pid
        int floorUsersPerPartition = numUsers / pids.size();

        int index = 0;
        for(TIntIterator iter = pids.iterator(); iter.hasNext(); ) {
            int i = iter.next();
            Arrays.fill(replicatedPids, index, index + floorUsersPerPartition, i);
            index += floorUsersPerPartition;
        }

        //Step 2: fill the remainder (if any) with randomly-selected pids (no more than once each)
        int[] remainingPidArray = pids.toArray();
        shuffle(remainingPidArray);
        int nextPidIndex = 0;
        while(index < replicatedPids.length) {
            replicatedPids[index] = remainingPidArray[nextPidIndex++];
            index++;
        }

        return replicatedPids;
    }

    static class State {
        private final float alpha;
        private final TIntObjectMap<TIntSet> friendships;
        private TIntObjectMap<TIntIntMap> uidToPidToFriendCounts;

        private TIntIntMap logicalPids;

        public State(float alpha, TIntObjectMap<TIntSet> friendships) {
            this.alpha = alpha;
            this.friendships = friendships;
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

        public TIntObjectMap<TIntIntMap> getUidToPidToFriendCounts() {
            return uidToPidToFriendCounts;
        }

        public void initUidToPidToFriendCount(TIntObjectMap<TIntSet> partitions) {
            uidToPidToFriendCounts = new TIntObjectHashMap<>(friendships.size()+1);
            for(int uid : friendships.keys()) {
                TIntIntMap counts = new TIntIntHashMap(partitions.size()+1);
                for(int pid : partitions.keys()) {

                    counts.put(pid, intersection(partitions.get(pid), friendships.get(uid)).size());
                }
                uidToPidToFriendCounts.put(uid, counts);
            }
        }
    }

}
