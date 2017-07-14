package io.vntr.migration;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.repartition.Target;
import io.vntr.utils.TroveUtils;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/24/17.
 */

public class SMigrator {

    public static TIntIntMap getUserMigrationStrategy(Integer pid, TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas, boolean smartPlacement) {
        //Reallocate the N/M master nodes hosted in that server to the remaining M-1 servers equally.
        //Decide the server in which a slave replica is promoted to master, based on the ratio of its neighbors that already exist on that server.
        //Thus, highly connected nodes, with potentially many replicas to be moved due to local data semantics, get to first choose the server they go to.
        //Place the remaining nodes wherever they fit, following simple water-filling strategy.

        TIntIntMap pidToMasterCounts = getUserCounts(partitions);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        TIntObjectMap<TIntSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        TIntSet masterIds = partitions.get(pid);
        TIntIntMap remainingSpotsInPartitions = getRemainingSpotsInPartitions(singleton(pid), uidToPidMap.size(), pidToMasterCounts);
        TIntIntMap strategy = new TIntIntHashMap(masterIds.size() + 1);

        //First, attempt to place vertices on partitions where they have a large proportion of their friends
        if(smartPlacement) {
            NavigableSet<Target> targets = new TreeSet<>();
            for(TIntIterator iter = masterIds.iterator(); iter.hasNext(); ) {
                int uid = iter.next();
                for(TIntIterator iter2 = uidToReplicasMap.get(uid).iterator(); iter2.hasNext(); ) {
                    int replicaPid = iter2.next();
                    float score = scoreReplicaPromotion(friendships.get(uid), partitions.get(replicaPid));
                    Target target = new Target(uid, replicaPid, pid, score);
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

        //Second, place remaining users on the lightest partition where they have a replica
        TIntSet usersYetUnplaced = new TIntHashSet(masterIds);
        usersYetUnplaced.removeAll(strategy.keySet());
        TIntIntMap numOnEachPartition = getNumberOnEachPartition(partitions, strategy);

        for(TIntIterator iter = usersYetUnplaced.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            TIntSet replicaPids = uidToReplicasMap.get(uid);
            if(replicaPids.size() > 0) {
                int lightestPid = -1;
                int minWeight = Integer.MAX_VALUE;
                for(TIntIterator iter2 = uidToReplicasMap.get(uid).iterator(); iter2.hasNext(); ) {
                    int curPid = iter2.next();
                    int weight = numOnEachPartition.get(curPid);
                    if(weight < minWeight) {
                        minWeight = weight;
                        lightestPid = curPid;
                    }
                }
                strategy.put(uid, lightestPid);
                iter.remove();
            }
        }

        //Finally, place remaining users at random (only relevant if minNumReplicas == 0)
        WaterFillingPriorityQueue priorityQueue = new WaterFillingPriorityQueue(partitions, strategy, pid);

        for(TIntIterator iter = usersYetUnplaced.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            int newPid = priorityQueue.getNextPid();
            strategy.put(uid, newPid);
        }

        return strategy;
    }

    static TIntIntMap getNumberOnEachPartition(TIntObjectMap<TIntSet> partitions, TIntIntMap strategy) {
        TIntIntMap numOnEachPartition = TroveUtils.getUserCounts(partitions);
        TIntObjectMap<TIntSet> reverseStrategy = TroveUtils.invertTIntIntMap(strategy);
        TIntIntMap numMovingToEachPartition = TroveUtils.getUserCounts(reverseStrategy);
        for(int pid : numMovingToEachPartition.keys()) {
            numOnEachPartition.put(pid, numOnEachPartition.get(pid) + numMovingToEachPartition.get(pid));
        }
        return numOnEachPartition;
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

    static float scoreReplicaPromotion(TIntSet friendIds, TIntSet usersOnPartition) {
        //based on what they've said, it seems like a decent scoring mechanism is numFriendsOnPartition^2 / numFriendsTotal
        int numFriendsOnPartition = intersection(friendIds, usersOnPartition).size();
        float numFriendsTotal = friendIds.size();

        return ((float) (numFriendsOnPartition * numFriendsOnPartition)) / numFriendsTotal;
    }

    static TIntIntMap getRemainingSpotsInPartitions(TIntSet pidsToSkip, int numUsers, TIntIntMap pToMasterCounts) {
        int numPartitions = pToMasterCounts.size() - pidsToSkip.size();
        int maxUsersPerPartition = numUsers / numPartitions;
        if (numUsers % numPartitions != 0) {
            maxUsersPerPartition++;
        }

        TIntIntMap remainingSpotsInPartitions = new TIntIntHashMap(pToMasterCounts.size()+1);
        for (Integer pid : pToMasterCounts.keys()) {
            if(pidsToSkip.contains(pid)) {
                continue;
            }
            remainingSpotsInPartitions.put(pid, maxUsersPerPartition - pToMasterCounts.get(pid));
        }

        return remainingSpotsInPartitions;
    }

}
