package io.vntr.repartition;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

import java.util.Arrays;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class JRepartitioner {

    public static NoRepResults repartition(float alpha, float initialT, float deltaT, short k, int numRestarts, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> friendships, boolean incremental) {
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        int bestEdgeCut = getEdgeCut(uidToPidMap, friendships);
        TShortShortMap bestLogicalPids = null;

        int logicalMigrationCount = 0;
        for (int i = 0; i < numRestarts; i++) {
            State state = initState(alpha, friendships);
            TShortObjectMap<TShortSet> logicalPartitions = incremental ? partitions : getRandomLogicalPartitions(friendships.keySet(), partitions.keySet());
            state.setLogicalPids(getUToMasterMap(logicalPartitions));
            state.initUidToPidToFriendCount(logicalPartitions);

            for(float t = initialT; t >= 1; t -= deltaT) {
                short[] randomUserArray = friendships.keys();
                shuffle(randomUserArray);
                for(short uid : randomUserArray) {
                    Short partnerId = null;
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
            }

            int edgeCut = getEdgeCut(state.getLogicalPids(), state.getFriendships());
            if(edgeCut < bestEdgeCut) {
                bestEdgeCut = edgeCut;
                bestLogicalPids = new TShortShortHashMap(state.getLogicalPids());
            }
        }

        return new NoRepResults(bestLogicalPids, logicalMigrationCount);
    }

    static Short findPartner(short uid, TShortSet candidates, float t, State state) {
        Short bestPartner = null;
        float bestScore = 0f;

        short logicalPid = state.getLogicalPids().get(uid);

        for(TShortIterator iter = candidates.iterator(); iter.hasNext(); ) {
            short partnerId = iter.next();
            Short theirLogicalPid = state.getLogicalPids().get(partnerId);
            if(theirLogicalPid.equals(logicalPid)) {
                continue;
            }

            short myNeighborsOnMine      = state.getUidToPidToFriendCounts().get(uid).get(logicalPid);
            short myNeighborsOnTheirs    = state.getUidToPidToFriendCounts().get(uid).get(theirLogicalPid);
            short theirNeighborsOnMine   = state.getUidToPidToFriendCounts().get(partnerId).get(logicalPid);
            short theirNeighborsOnTheirs = state.getUidToPidToFriendCounts().get(partnerId).get(theirLogicalPid);

            float oldScore = (float) (Math.pow(myNeighborsOnMine, state.getAlpha()) + Math.pow(theirNeighborsOnTheirs, state.getAlpha()));
            float newScore = (float) (Math.pow(myNeighborsOnTheirs, state.getAlpha()) + Math.pow(theirNeighborsOnMine, state.getAlpha()));

            if(newScore > bestScore && (newScore * t) > oldScore) {
                bestPartner = partnerId;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    static short[] howManyFriendsHaveLogicalPartitions(short uid, short[] pids, State state) {
        short[] counts = new short[pids.length];
        for(TShortIterator iter = state.getFriendships().get(uid).iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            short friendPid = state.getLogicalPids().get(friendId);
            for(int i=0; i<pids.length; i++) {
                if(pids[i] == friendPid) {
                    counts[i]++;
                }
            }
        }
        return counts;
    }

    static TShortSet sample(short n, TShortSet uids) {
        return uids.size() > n ? getKDistinctValuesFromArray(n, uids.toArray()) : new TShortHashSet(uids);
    }

    static void logicalSwap(short uid1, short uid2, State state) {
        short pid1 = state.getLogicalPids().get(uid1);
        short pid2 = state.getLogicalPids().get(uid2);

        state.getLogicalPids().put(uid1, pid2);
        state.getLogicalPids().put(uid2, pid1);

        for(TShortIterator iter = state.getFriendships().get(uid1).iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            TShortShortMap counts = state.getUidToPidToFriendCounts().get(friendId);
            counts.put(pid1, (short)(counts.get(pid1) - 1));
            counts.put(pid2, (short)(counts.get(pid2) + 1));
        }

        for(TShortIterator iter = state.getFriendships().get(uid1).iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            TShortShortMap counts = state.getUidToPidToFriendCounts().get(friendId);
            counts.put(pid2, (short)(counts.get(pid2) - 1));
            counts.put(pid1, (short)(counts.get(pid1) + 1));
        }

    }

    static State initState(float alpha, TShortObjectMap<TShortSet> friendships) {
        TShortObjectMap<TShortSet> friendshipsCopy = new TShortObjectHashMap<>(friendships.size()+1);
        for(short uid : friendships.keys()) {
            friendshipsCopy.put(uid, new TShortHashSet(friendships.get(uid)));
        }
        return new State(alpha, friendshipsCopy);
    }

    static int getEdgeCut(TShortShortMap uidToPidMap, TShortObjectMap<TShortSet> friendships) {
        int count = 0;
        for(short uid : uidToPidMap.keys()) {
            short pid = uidToPidMap.get(uid);

            for(TShortIterator iter = friendships.get(uid).iterator(); iter.hasNext(); ) {
                short friendId = iter.next();
                short friendPid = uidToPidMap.get(friendId);
                if(pid < friendPid) {
                    count++;
                }
            }
        }
        return count;
    }

    static TShortObjectMap<TShortSet> getRandomLogicalPartitions(TShortSet uids, TShortSet pids) {
        short[] pidArray = getPidsToAssign(uids.size(), pids);
        shuffle(pidArray);

        TShortObjectMap<TShortSet> logicalPartitions = new TShortObjectHashMap<>(pids.size()+1);
        for(TShortIterator iter = pids.iterator(); iter.hasNext(); ) {
            short pid = iter.next();
            logicalPartitions.put(pid, new TShortHashSet());
        }

        int index = 0;
        for(TShortIterator iter = uids.iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            logicalPartitions.get(pidArray[index]).add(uid);
            index++;
        }

        return logicalPartitions;
    }

    static short[] getPidsToAssign(int numUsers, TShortSet pids) {
        //Fill array with pids such that:
        //(1) array.length = numUsers
        //(2) no pid occurs more than ceiling(numUsers/numPartitions) times
        //Note: The order is unimportant, since we'll shuffle it later
        short[] replicatedPids = new short[numUsers];

        //Step 1: fill the first numPartitions * floor(numUsers/numPartitions) elements
        //This is easy, because we can just put floor(numUsers/numPartitions) copies of each pid
        short floorUsersPerPartition = (short)(numUsers / pids.size());

        int index = 0;
        for(TShortIterator iter = pids.iterator(); iter.hasNext(); ) {
            short i = iter.next();
            Arrays.fill(replicatedPids, index, index + floorUsersPerPartition, i);
            index += floorUsersPerPartition;
        }

        //Step 2: fill the remainder (if any) with randomly-selected pids (no more than once each)
        short[] remainingPidArray = pids.toArray();
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
        private final TShortObjectMap<TShortSet> friendships;
        private TShortObjectMap<TShortShortMap> uidToPidToFriendCounts;

        private TShortShortMap logicalPids;

        public State(float alpha, TShortObjectMap<TShortSet> friendships) {
            this.alpha = alpha;
            this.friendships = friendships;
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

        public TShortObjectMap<TShortShortMap> getUidToPidToFriendCounts() {
            return uidToPidToFriendCounts;
        }

        public void initUidToPidToFriendCount(TShortObjectMap<TShortSet> partitions) {
            uidToPidToFriendCounts = new TShortObjectHashMap<>(friendships.size() + 1);
            for(short uid : friendships.keys()) {
                TShortShortMap counts = new TShortShortHashMap(partitions.size()+1);
                for(short pid : partitions.keys()) {
                    counts.put(pid, (short) intersection(partitions.get(pid), friendships.get(uid)).size());
                }
                uidToPidToFriendCounts.put(uid, counts);
            }
        }
    }

}
