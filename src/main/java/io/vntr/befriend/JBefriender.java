package io.vntr.befriend;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class JBefriender {

    public static Result rebalance(Integer smallerUid, Integer largerUid, int k, float alpha, TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> partitions) {
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);

        int smallerPid = uidToPidMap.get(smallerUid);
        int largerPid = uidToPidMap.get(largerUid);

        if (smallerPid != largerPid) {
            Integer smallerPartnerId = findPartnerOnPartition(smallerUid, partitions.get(largerPid), k, alpha, friendships, uidToPidMap);
            Integer largerPartnerId = findPartnerOnPartition(largerUid, partitions.get(smallerPid), k, alpha, friendships, uidToPidMap);

            if (smallerPartnerId != null && largerPartnerId == null) {
                return new Result(smallerUid, smallerPartnerId);
            } else if (largerPartnerId != null && smallerPartnerId == null) {
                return new Result(largerUid, largerPartnerId);
            } else if (smallerPartnerId != null && largerPartnerId != null) {
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

    static Integer findPartnerOnPartition(int uid, TIntSet partition, int k, float alpha, TIntObjectMap<TIntSet> friendships, TIntIntMap uidToPidMap) {
        TIntSet candidates;
        if (partition.size() <= k) {
            candidates = new TIntHashSet(partition);
        } else {
            candidates = getKDistinctValuesFromArray(k, partition.toArray());
        }
        return findPartner(uid, candidates, alpha, friendships, uidToPidMap);
    }

    static int calculateGain(int uid1, int uid2, TIntObjectMap<TIntSet> friendships, TIntIntMap uidToPidMap) {
        boolean u1AndU2AreFriends = friendships.get(uid1).contains(uid2);
        TIntSet u1Friends = friendships.get(uid1);
        TIntSet u2Friends = friendships.get(uid2);
        int pid1 = uidToPidMap.get(uid1);
        int pid2 = uidToPidMap.get(uid2);
        int oldCut = getNeighborsOnPartition(u1Friends, pid2, uidToPidMap) + getNeighborsOnPartition(u2Friends, pid1, uidToPidMap);
        int newCut = getNeighborsOnPartition(u1Friends, pid1, uidToPidMap) + getNeighborsOnPartition(u2Friends, pid2, uidToPidMap);
        return oldCut - newCut - (u1AndU2AreFriends ? 2 : 0);
    }

    static int getNeighborsOnPartition(TIntSet friendIds, Integer pid, TIntIntMap uidToPidMap) {
        int count = 0;
        for(TIntIterator iter = friendIds.iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            count += uidToPidMap.get(friendId) == pid ? 1 : 0;
        }

        return count;
    }

    static Integer findPartner(int uid, TIntSet candidates, float alpha, TIntObjectMap<TIntSet> friendships, TIntIntMap uidToPidMap) {
        Integer bestPartnerId = null;
        float bestScore = 0f;

        Integer myPid = uidToPidMap.get(uid);

        for(TIntIterator iter = candidates.iterator(); iter.hasNext(); ) {
            int partnerId = iter.next();
            Integer theirPid = uidToPidMap.get(partnerId);
            if (theirPid.equals(myPid)) {
                continue;
            }

            boolean uAndPartnerAreFriends = friendships.get(uid).contains(partnerId);

            int myNeighborsOnMine = howManyFriendsHavePartition(friendships.get(uid), myPid, uidToPidMap);
            int myNeighborsOnTheirs = howManyFriendsHavePartition(friendships.get(uid), theirPid, uidToPidMap) - (uAndPartnerAreFriends ? 1 : 0);
            int theirNeighborsOnMine = howManyFriendsHavePartition(friendships.get(partnerId), myPid, uidToPidMap) - (uAndPartnerAreFriends ? 1 : 0);
            int theirNeighborsOnTheirs = howManyFriendsHavePartition(friendships.get(partnerId), theirPid, uidToPidMap);

            float oldScore = (float) (Math.pow(myNeighborsOnMine, alpha) + Math.pow(theirNeighborsOnTheirs, alpha));
            float newScore = (float) (Math.pow(myNeighborsOnTheirs, alpha) + Math.pow(theirNeighborsOnMine, alpha));

            if (newScore > bestScore && newScore > oldScore) {
                bestPartnerId = partnerId;
                bestScore = newScore;
            }
        }

        return bestPartnerId;
    }

    static int howManyFriendsHavePartition(TIntSet friendIds, Integer pid, TIntIntMap uidToPidMap) {
        int count = 0;
        for(TIntIterator iter = friendIds.iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            count += uidToPidMap.get(friendId) == pid ? 1 : 0;
        }
        return count;
    }

    public static class Result {
        private final Integer uid1;
        private final Integer uid2;

        public Result(Integer uid1, Integer uid2) {
            this.uid1 = uid1;
            this.uid2 = uid2;
        }

        public Integer getUid1() {
            return uid1;
        }

        public Integer getUid2() {
            return uid2;
        }
    }

}