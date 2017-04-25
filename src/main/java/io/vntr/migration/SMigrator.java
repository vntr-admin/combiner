package io.vntr.migration;


import java.util.*;

import static io.vntr.Utils.*;
import static java.util.Collections.singleton;

/**
 * Created by robertlindquist on 4/24/17.
 */

public class SMigrator {

    public static Map<Integer, Integer> getUserMigrationStrategy(Integer partitionId, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        //Reallocate the N/M master nodes hosted in that server to the remaining M-1 servers equally.
        //Decide the server in which a slave replica is promoted to master, based on the ratio of its neighbors that already exist on that server.
        //Thus, highly connected nodes, with potentially many replicas to be moved due to local data semantics, get to first choose the server they go to.
        //Place the remaining nodes wherever they fit, following simple water-filling strategy.

        Map<Integer, Integer> pidToMasterCounts = getUserCounts(partitions);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        Set<Integer> masterIds = partitions.get(partitionId);

        NavigableSet<Score> scores = new TreeSet<>();
        for (Integer userId : masterIds) {
            for (Integer replicaPartitionId : uidToReplicasMap.get(userId)) {
                float score = scoreReplicaPromotion(friendships.get(userId), replicaPartitionId, uidToPidMap);
                scores.add(new Score(userId, replicaPartitionId, score));
            }
        }

        Map<Integer, Integer> remainingSpotsInPartitions = getRemainingSpotsInPartitions(singleton(partitionId), uidToPidMap.size(), pidToMasterCounts);

        Map<Integer, Integer> strategy = new HashMap<>();

        for (Iterator<Score> iter = scores.descendingIterator(); iter.hasNext(); ) {
            Score score = iter.next();
            int remainingSpotsInPartition = remainingSpotsInPartitions.get(score.partitionId);
            if (!strategy.containsKey(score.userId) && remainingSpotsInPartition > 0 && score.score > 0) {
                strategy.put(score.userId, score.partitionId);
                remainingSpotsInPartitions.put(score.partitionId, remainingSpotsInPartition - 1);
            }
        }

        Set<Integer> usersYetUnplaced = new HashSet<>(masterIds);
        usersYetUnplaced.removeAll(strategy.keySet());

        for (Integer uid : usersYetUnplaced) {
            Integer targetPid = getLeastOverloadedPartitionWhereThisUserHasAReplica(uidToReplicasMap.get(uid), strategy, pidToMasterCounts);
            if(targetPid != null) {
                strategy.put(uid, targetPid);
            }
            else {
                strategy.put(uid, getLeastOverloadedPartition(strategy, partitionId, pidToMasterCounts));
            }
        }

        return strategy;
    }

    static Integer getLeastOverloadedPartition(Map<Integer, Integer> strategy, Integer pidToDelete, Map<Integer, Integer> pToMasterCounts) {
        Map<Integer, Integer> pToStrategyCount = new HashMap<>();
        for(Integer pid : pToMasterCounts.keySet()) {
            pToStrategyCount.put(pid, 0);
        }
        for(Integer uid1 : strategy.keySet()) {
            int pid = strategy.get(uid1);
            pToStrategyCount.put(pid, pToStrategyCount.get(pid));
        }

        int minMasters = Integer.MAX_VALUE;
        Integer minPid = null;
        for(Integer pid : pToMasterCounts.keySet()) {
            if(pid.equals(pidToDelete)) {
                continue;
            }
            int numMasters = pToMasterCounts.get(pid) + pToStrategyCount.get(pid);
            if(numMasters < minMasters) {
                minMasters = numMasters;
                minPid = pid;
            }
        }
        return minPid;
    }

    static Integer getLeastOverloadedPartitionWhereThisUserHasAReplica(Set<Integer> replicaPids, Map<Integer, Integer> strategy, Map<Integer, Integer> pToMasterCounts) {
        Map<Integer, Integer> pToStrategyCount = new HashMap<>();
        for(Integer pid : pToMasterCounts.keySet()) {
            pToStrategyCount.put(pid, 0);
        }
        for(Integer uid1 : strategy.keySet()) {
            int pid = strategy.get(uid1);
            pToStrategyCount.put(pid, pToStrategyCount.get(pid));
        }

        int minMasters = Integer.MAX_VALUE;
        Integer minPid = null;
        for(Integer pid : replicaPids) {
            int numMasters = pToMasterCounts.get(pid) + pToStrategyCount.get(pid);
            if(numMasters < minMasters) {
                minMasters = numMasters;
                minPid = pid;
            }
        }
        return minPid;
    }

    static float scoreReplicaPromotion(Set<Integer> friendIds, Integer replicaPartitionId, Map<Integer, Integer> uidToPidMap) {
        //based on what they've said, it seems like a decent scoring mechanism is numFriendsOnPartition^2 / numFriendsTotal
        int numFriendsOnPartition = 0;
        for (Integer friendId : friendIds) {
            int friendPid = uidToPidMap.get(friendId);
            if (friendPid == replicaPartitionId) {
                numFriendsOnPartition++;
            }
        }

        int numFriendsTotal = friendIds.size();

        return ((float) (numFriendsOnPartition * numFriendsOnPartition)) / (numFriendsTotal);
    }

    static Map<Integer, Integer> getRemainingSpotsInPartitions(Set<Integer> partitionIdsToSkip, int numUsers, Map<Integer, Integer> pToMasterCounts) {
        int numPartitions = pToMasterCounts.size() - partitionIdsToSkip.size();
        int maxUsersPerPartition = numUsers / numPartitions;
        if (numUsers % numPartitions != 0) {
            maxUsersPerPartition++;
        }

        Map<Integer, Integer> remainingSpotsInPartitions = new HashMap<>();
        for (Integer partitionId : pToMasterCounts.keySet()) {
            if(partitionIdsToSkip.contains(partitionId)) {
                continue;
            }
            remainingSpotsInPartitions.put(partitionId, maxUsersPerPartition - pToMasterCounts.get(partitionId));
        }

        return remainingSpotsInPartitions;
    }

    private static class Score implements Comparable<Score> {
        public final int userId;
        public final int partitionId;
        public final float score;

        public Score(Integer userId, Integer partitionId, float score) {
            this.userId = userId;
            this.partitionId = partitionId;
            this.score = score;
        }

        public int compareTo(Score o) {
            if (this.score > o.score) {
                return 1;
            } else if (this.score < o.score) {
                return -1;
            } else {
                if (this.partitionId > o.partitionId) {
                    return 1;
                } else if (this.partitionId > o.partitionId) {
                    return -1;
                } else {
                    if (this.userId > o.userId) {
                        return 1;
                    } else if (this.userId < o.userId) {
                        return -1;
                    }

                    return 0;
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Score score1 = (Score) o;

            if (userId != score1.userId) return false;
            if (partitionId != score1.partitionId) return false;
            return Float.compare(score1.score, score) == 0;

        }

        @Override
        public int hashCode() {
            int result = userId;
            result = 31 * result + partitionId;
            result = 31 * result + (score != +0.0f ? Float.floatToIntBits(score) : 0);
            return result;
        }

        @Override
        public String toString() {
            return String.format("%3d: --(%.2f)--> %3d", userId, score, partitionId);
        }
    }
}
