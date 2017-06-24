package io.vntr.migration;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.repartition.Target;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/24/17.
 */

public class SMigrator {

    public static TIntIntMap getUserMigrationStrategy(Integer partitionId, TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas, boolean scoreTargets) {
        //Reallocate the N/M master nodes hosted in that server to the remaining M-1 servers equally.
        //Decide the server in which a slave replica is promoted to master, based on the ratio of its neighbors that already exist on that server.
        //Thus, highly connected nodes, with potentially many replicas to be moved due to local data semantics, get to first choose the server they go to.
        //Place the remaining nodes wherever they fit, following simple water-filling strategy.

        TIntIntMap pidToMasterCounts = getUserCounts(partitions);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        TIntSet masterIds = partitions.get(partitionId);
        TIntIntMap remainingSpotsInPartitions = getRemainingSpotsInPartitions(singleton(partitionId), uidToPidMap.size(), pidToMasterCounts);
        TIntIntMap strategy = new TIntIntHashMap(masterIds.size() + 1);

        if(scoreTargets) {
            NavigableSet<Target> targets = new TreeSet<>();
            for(TIntIterator iter = masterIds.iterator(); iter.hasNext(); ) {
                int userId = iter.next();
                for(TIntIterator iter2 = uidToReplicasMap.get(userId).iterator(); iter2.hasNext(); ) {
                    int replicaPartitionId = iter2.next();
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

        TIntSet usersYetUnplaced = new TIntHashSet(masterIds);
        usersYetUnplaced.removeAll(strategy.keySet());

        for(TIntIterator iter = usersYetUnplaced.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
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

    static Integer getLeastOverloadedPartition(TIntIntMap strategy, Integer pidToDelete, TIntIntMap pToMasterCounts) {
        TIntIntMap pToStrategyCount = new TIntIntHashMap(pToMasterCounts.size()+1);
        for(Integer pid : pToMasterCounts.keys()) {
            pToStrategyCount.put(pid, 0);
        }
        for(Integer uid1 : strategy.keys()) {
            int pid = strategy.get(uid1);
            pToStrategyCount.put(pid, pToStrategyCount.get(pid) + 1);
        }

        int minMasters = Integer.MAX_VALUE;
        Integer minPid = null;
        for(Integer pid : pToMasterCounts.keys()) {
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

    static Integer getLeastOverloadedPartitionWhereThisUserHasAReplica(TIntSet replicaPids, TIntIntMap strategy, TIntIntMap pToMasterCounts) {
        TIntIntMap pToStrategyCount = new TIntIntHashMap(pToMasterCounts.size()+1);
        for(Integer pid : pToMasterCounts.keys()) {
            pToStrategyCount.put(pid, 0);
        }
        for(Integer uid1 : strategy.keys()) {
            int pid = strategy.get(uid1);
            pToStrategyCount.put(pid, pToStrategyCount.get(pid) + 1);
        }

        int minMasters = Integer.MAX_VALUE;
        Integer minPid = null;
        for(TIntIterator iter = replicaPids.iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            int numMasters = pToMasterCounts.get(pid) + pToStrategyCount.get(pid);
            if(numMasters < minMasters) {
                minMasters = numMasters;
                minPid = pid;
            }
        }
        return minPid;
    }

    static float scoreReplicaPromotion(TIntSet friendIds, TIntSet usersOnPartition) {
        //based on what they've said, it seems like a decent scoring mechanism is numFriendsOnPartition^2 / numFriendsTotal
        int numFriendsOnPartition = intersection(friendIds, usersOnPartition).size();
        float numFriendsTotal = friendIds.size();

        return ((float) (numFriendsOnPartition * numFriendsOnPartition)) / numFriendsTotal;
    }

    static TIntIntMap getRemainingSpotsInPartitions(TIntSet partitionIdsToSkip, int numUsers, TIntIntMap pToMasterCounts) {
        int numPartitions = pToMasterCounts.size() - partitionIdsToSkip.size();
        int maxUsersPerPartition = numUsers / numPartitions;
        if (numUsers % numPartitions != 0) {
            maxUsersPerPartition++;
        }

        TIntIntMap remainingSpotsInPartitions = new TIntIntHashMap(pToMasterCounts.size()+1);
        for (Integer partitionId : pToMasterCounts.keys()) {
            if(partitionIdsToSkip.contains(partitionId)) {
                continue;
            }
            remainingSpotsInPartitions.put(partitionId, maxUsersPerPartition - pToMasterCounts.get(partitionId));
        }

        return remainingSpotsInPartitions;
    }

}
