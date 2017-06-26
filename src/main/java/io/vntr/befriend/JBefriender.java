package io.vntr.befriend;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class JBefriender {

    public static Result rebalance(short smallerUid, short largerUid, short k, float alpha, TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> partitions) {
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);

        short smallerPid = uidToPidMap.get(smallerUid);
        short largerPid = uidToPidMap.get(largerUid);

        if (smallerPid != largerPid) {
            Short smallerPartnerId = findPartnerOnPartition(smallerUid, partitions.get(largerPid), k, alpha, friendships, uidToPidMap);
            Short largerPartnerId = findPartnerOnPartition(largerUid, partitions.get(smallerPid), k, alpha, friendships, uidToPidMap);

            if (smallerPartnerId != null && largerPartnerId == null) {
                return new Result(smallerUid, smallerPartnerId);
            } else if (largerPartnerId != null && smallerPartnerId == null) {
                return new Result(largerUid, largerPartnerId);
            } else if (smallerPartnerId != null) {
                int gainSmallerToLarger = calculateGain(smallerPartnerId, largerPartnerId, friendships, uidToPidMap);
                int gainLargerToSmaller = calculateGain(largerPartnerId, smallerPartnerId, friendships, uidToPidMap);
                if (gainSmallerToLarger >= gainLargerToSmaller) {
                    return new Result(smallerUid, smallerPartnerId);
                } else {
                    return new Result(largerUid, largerPartnerId);
                }
            }
        }
        return new Result(null, null);
    }

    static Short findPartnerOnPartition(short uid, TShortSet partition, short k, float alpha, TShortObjectMap<TShortSet> friendships, TShortShortMap uidToPidMap) {
        TShortSet candidates;
        if (partition.size() <= k) {
            candidates = new TShortHashSet(partition);
        } else {
            candidates = getKDistinctValuesFromArray(k, partition.toArray());
        }
        return findPartner(uid, candidates, alpha, friendships, uidToPidMap);
    }

    static int calculateGain(short uid1, short uid2, TShortObjectMap<TShortSet> friendships, TShortShortMap uidToPidMap) {
        boolean u1AndU2AreFriends = friendships.get(uid1).contains(uid2);
        TShortSet u1Friends = friendships.get(uid1);
        TShortSet u2Friends = friendships.get(uid2);
        short pid1 = uidToPidMap.get(uid1);
        short pid2 = uidToPidMap.get(uid2);
        int oldCut = getNeighborsOnPartition(u1Friends, pid2, uidToPidMap) + getNeighborsOnPartition(u2Friends, pid1, uidToPidMap);
        int newCut = getNeighborsOnPartition(u1Friends, pid1, uidToPidMap) + getNeighborsOnPartition(u2Friends, pid2, uidToPidMap);
        return oldCut - newCut - (u1AndU2AreFriends ? 2 : 0);
    }

    static int getNeighborsOnPartition(TShortSet friendIds, short pid, TShortShortMap uidToPidMap) {
        int count = 0;
        for(TShortIterator iter = friendIds.iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            count += uidToPidMap.get(friendId) == pid ? 1 : 0;
        }

        return count;
    }

    static Short findPartner(short uid, TShortSet candidates, float alpha, TShortObjectMap<TShortSet> friendships, TShortShortMap uidToPidMap) {
        Short bestPartnerId = null;
        float bestScore = 0f;

        short myPid = uidToPidMap.get(uid);

        for(TShortIterator iter = candidates.iterator(); iter.hasNext(); ) {
            short partnerId = iter.next();
            short theirPid = uidToPidMap.get(partnerId);
            if (theirPid == myPid) {
                continue;
            }

            short friendAdjustment = friendships.get(uid).contains(partnerId) ? (short) 1 : (short) 0;

            short myNeighborsOnMine = howManyFriendsHavePartition(friendships.get(uid), myPid, uidToPidMap);
            short myNeighborsOnTheirs = (short)(howManyFriendsHavePartition(friendships.get(uid), theirPid, uidToPidMap) - friendAdjustment);
            short theirNeighborsOnMine = (short)(howManyFriendsHavePartition(friendships.get(partnerId), myPid, uidToPidMap) - friendAdjustment);
            short theirNeighborsOnTheirs = howManyFriendsHavePartition(friendships.get(partnerId), theirPid, uidToPidMap);

            float oldScore = (float) (Math.pow(myNeighborsOnMine, alpha) + Math.pow(theirNeighborsOnTheirs, alpha));
            float newScore = (float) (Math.pow(myNeighborsOnTheirs, alpha) + Math.pow(theirNeighborsOnMine, alpha));

            if (newScore > bestScore && newScore > oldScore) {
                bestPartnerId = partnerId;
                bestScore = newScore;
            }
        }

        return bestPartnerId;
    }

    static short howManyFriendsHavePartition(TShortSet friendIds, short pid, TShortShortMap uidToPidMap) {
        short count = 0;
        for(TShortIterator iter = friendIds.iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            count += uidToPidMap.get(friendId) == pid ? 1 : 0;
        }
        return count;
    }

    public static class Result {
        private final Short uid1;
        private final Short uid2;

        public Result(Short uid1, Short uid2) {
            this.uid1 = uid1;
            this.uid2 = uid2;
        }

        public Short getUid1() {
            return uid1;
        }

        public Short getUid2() {
            return uid2;
        }
    }

}