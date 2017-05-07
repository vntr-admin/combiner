package io.vntr.migration;

import com.google.common.collect.Sets;
import io.vntr.repartition.Target;

import java.util.*;

import static io.vntr.utils.Utils.*;
import static java.util.Collections.singleton;

/**
 * Created by robertlindquist on 4/24/17.
 */

public class SMigrator {

    public static Map<Integer, Integer> getUserMigrationStrategy(Integer partitionId, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas, boolean scoreTargets) {
        //Reallocate the N/M master nodes hosted in that server to the remaining M-1 servers equally.
        //Decide the server in which a slave replica is promoted to master, based on the ratio of its neighbors that already exist on that server.
        //Thus, highly connected nodes, with potentially many replicas to be moved due to local data semantics, get to first choose the server they go to.
        //Place the remaining nodes wherever they fit, following simple water-filling strategy.

        Map<Integer, Integer> pidToMasterCounts = getUserCounts(partitions);
        Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
        Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        Set<Integer> masterIds = partitions.get(partitionId);
        Map<Integer, Integer> remainingSpotsInPartitions = getRemainingSpotsInPartitions(singleton(partitionId), uidToPidMap.size(), pidToMasterCounts);
        Map<Integer, Integer> strategy = new HashMap<>();

        if(scoreTargets) {
            NavigableSet<Target> targets = new TreeSet<>();
            for (Integer userId : masterIds) {
                for (Integer replicaPartitionId : uidToReplicasMap.get(userId)) {
                    float score = scoreReplicaPromotion(friendships.get(userId), partitions.get(replicaPartitionId));
                    Target target = new Target(userId, replicaPartitionId, partitionId, score);
                    targets.add(target);
                }
            }


            for (Iterator<Target> iter = targets.descendingIterator(); iter.hasNext(); ) {
                Target target = iter.next();
                int remainingSpotsInPartition = remainingSpotsInPartitions.get(target.pid);
                if (!strategy.containsKey(target.uid) && remainingSpotsInPartition > 0) {
                    strategy.put(target.uid, target.pid);
                    remainingSpotsInPartitions.put(target.pid, remainingSpotsInPartition - 1);
                }
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
            pToStrategyCount.put(pid, pToStrategyCount.get(pid) + 1);
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
            pToStrategyCount.put(pid, pToStrategyCount.get(pid) + 1);
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

    static float scoreReplicaPromotion(Set<Integer> friendIds, Set<Integer> usersOnPartition) {
        //based on what they've said, it seems like a decent scoring mechanism is numFriendsOnPartition^2 / numFriendsTotal
        int numFriendsOnPartition = Sets.intersection(friendIds, usersOnPartition).size();
        float numFriendsTotal = friendIds.size();

        return ((float) (numFriendsOnPartition * numFriendsOnPartition)) / numFriendsTotal;
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

}
