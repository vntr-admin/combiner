package io.vntr.befriend;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.vntr.utils.Utils.getUToMasterMap;
import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;

/**
 * Created by robertlindquist on 4/25/17.
 */
public class JBefriender {

    public static Result rebalance(Integer smallerUserId, Integer largerUserId, int k, float alpha, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) {
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);

        int smallerPid = uidToPidMap.get(smallerUserId);
        int largerPid = uidToPidMap.get(largerUserId);

        if (smallerPid != largerPid) {
            Integer smallerPartnerId = findPartnerOnPartition(smallerUserId, partitions.get(largerPid), k, alpha, friendships, uidToPidMap);
            Integer largerPartnerId = findPartnerOnPartition(largerUserId, partitions.get(smallerPid), k, alpha, friendships, uidToPidMap);

            if (smallerPartnerId != null && largerPartnerId == null) {
                return new Result(smallerUserId, smallerPartnerId);
            } else if (largerPartnerId != null && smallerPartnerId == null) {
                return new Result(largerUserId, largerPartnerId);
            } else if (smallerPartnerId != null && largerPartnerId != null) {
                int gainSmallerToLarger = calculateGain(smallerPartnerId, largerPartnerId, friendships, uidToPidMap);
                int gainLargerToSmaller = calculateGain(largerPartnerId, smallerPartnerId, friendships, uidToPidMap);
                if (gainSmallerToLarger >= gainLargerToSmaller) {
                    return new Result(smallerUserId, smallerPartnerId);
                } else {
                    return new Result(largerUserId, largerPartnerId);
                }
            }
        }
        return new Result(null, null);
    }

    static Integer findPartnerOnPartition(int uid, Set<Integer> partition, int k, float alpha, Map<Integer, Set<Integer>> friendships, Map<Integer, Integer> uidToPidMap) {
        Set<Integer> candidates;
        if (partition.size() <= k) {
            candidates = new HashSet<>(partition);
        } else {
            candidates = getKDistinctValuesFromList(k, partition);
        }
        return findPartner(uid, candidates, alpha, friendships, uidToPidMap);
    }

    static int calculateGain(int uid1, int uid2, Map<Integer, Set<Integer>> friendships, Map<Integer, Integer> uidToPidMap) {
        boolean u1AndU2AreFriends = friendships.get(uid1).contains(uid2);
        Set<Integer> u1Friends = friendships.get(uid1);
        Set<Integer> u2Friends = friendships.get(uid2);
        int pid1 = uidToPidMap.get(uid1);
        int pid2 = uidToPidMap.get(uid2);
        int oldCut = getNeighborsOnPartition(u1Friends, pid2, uidToPidMap) + getNeighborsOnPartition(u2Friends, pid1, uidToPidMap);
        int newCut = getNeighborsOnPartition(u1Friends, pid1, uidToPidMap) + getNeighborsOnPartition(u2Friends, pid2, uidToPidMap);
        return oldCut - newCut - (u1AndU2AreFriends ? 2 : 0);
    }

    static int getNeighborsOnPartition(Set<Integer> friendIds, Integer pid, Map<Integer, Integer> uidToPidMap) {
        int count = 0;
        for (Integer friendId : friendIds) {
            count += uidToPidMap.get(friendId).equals(pid) ? 1 : 0;
        }

        return count;
    }

    static Integer findPartner(int uid, Set<Integer> candidates, float alpha, Map<Integer, Set<Integer>> friendships, Map<Integer, Integer> uidToPidMap) {
        Integer bestPartnerId = null;
        float bestScore = 0f;

        Integer myPid = uidToPidMap.get(uid);

        for (int partnerId : candidates) {
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

    static int howManyFriendsHavePartition(Set<Integer> friendIds, Integer pid, Map<Integer, Integer> uidToPidMap) {
        int count = 0;
        for (Integer friendId : friendIds) {
            count += uidToPidMap.get(friendId).equals(pid) ? 1 : 0;
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