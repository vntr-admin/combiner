package io.vntr.migration;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.repartition.Target;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/24/17.
 */

public class SMigrator {

    public static TShortShortMap getUserMigrationStrategy(short pid, TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas, boolean scoreTargets) {
        //Reallocate the N/M master nodes hosted in that server to the remaining M-1 servers equally.
        //Decide the server in which a slave replica is promoted to master, based on the ratio of its neighbors that already exist on that server.
        //Thus, highly connected nodes, with potentially many replicas to be moved due to local data semantics, get to first choose the server they go to.
        //Place the remaining nodes wherever they fit, following simple water-filling strategy.

        TShortShortMap pidToMasterCounts = getUserCounts(partitions);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> uidToReplicasMap = getUToReplicasMap(replicas, friendships.keySet());

        TShortSet masterIds = partitions.get(pid);
        TShortShortMap remainingSpotsInPartitions = getRemainingSpotsInPartitions(singleton(pid), uidToPidMap.size(), pidToMasterCounts);
        TShortShortMap strategy = new TShortShortHashMap(masterIds.size() + 1);

        if(scoreTargets) {
            NavigableSet<Target> targets = new TreeSet<>();
            for(TShortIterator iter = masterIds.iterator(); iter.hasNext(); ) {
                short uid = iter.next();
                for(TShortIterator iter2 = uidToReplicasMap.get(uid).iterator(); iter2.hasNext(); ) {
                    short replicaPid = iter2.next();
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
                    remainingSpotsInPartitions.put(target.pid, (short)(remainingSpotsInPartition - 1));
                }
            }
        }

        TShortSet usersYetUnplaced = new TShortHashSet(masterIds);
        usersYetUnplaced.removeAll(strategy.keySet());

        for(TShortIterator iter = usersYetUnplaced.iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            Short targetPid = getLeastOverloadedPartitionWhereThisUserHasAReplica(uidToReplicasMap.get(uid), strategy, pidToMasterCounts);
            if(targetPid != null) {
                strategy.put(uid, targetPid);
            }
            else {
                strategy.put(uid, getLeastOverloadedPartition(strategy, pid, pidToMasterCounts));
            }
        }

        return strategy;
    }

    static Short getLeastOverloadedPartition(TShortShortMap strategy, short pidToDelete, TShortShortMap pToMasterCounts) {
        TShortShortMap pToStrategyCount = new TShortShortHashMap(pToMasterCounts.size()+1);
        for(short pid : pToMasterCounts.keys()) {
            pToStrategyCount.put(pid, (short) 0);
        }
        for(short uid1 : strategy.keys()) {
            short pid = strategy.get(uid1);
            pToStrategyCount.put(pid, (short)(pToStrategyCount.get(pid) + 1));
        }

        int minMasters = Integer.MAX_VALUE;
        Short minPid = null;
        for(short pid : pToMasterCounts.keys()) {
            if(pid == pidToDelete) {
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

    static Short getLeastOverloadedPartitionWhereThisUserHasAReplica(TShortSet replicaPids, TShortShortMap strategy, TShortShortMap pToMasterCounts) {
        TShortShortMap pToStrategyCount = new TShortShortHashMap(pToMasterCounts.size()+1);
        for(short pid : pToMasterCounts.keys()) {
            pToStrategyCount.put(pid, (short) 0);
        }
        for(short uid1 : strategy.keys()) {
            short pid = strategy.get(uid1);
            pToStrategyCount.put(pid, (short)(pToStrategyCount.get(pid) + 1));
        }

        int minMasters = Integer.MAX_VALUE;
        Short minPid = null;
        for(TShortIterator iter = replicaPids.iterator(); iter.hasNext(); ) {
            short pid = iter.next();
            int numMasters = pToMasterCounts.get(pid) + pToStrategyCount.get(pid);
            if(numMasters < minMasters) {
                minMasters = numMasters;
                minPid = pid;
            }
        }
        return minPid;
    }

    static float scoreReplicaPromotion(TShortSet friendIds, TShortSet usersOnPartition) {
        //based on what they've said, it seems like a decent scoring mechanism is numFriendsOnPartition^2 / numFriendsTotal
        int numFriendsOnPartition = intersection(friendIds, usersOnPartition).size();
        float numFriendsTotal = friendIds.size();

        return ((float) (numFriendsOnPartition * numFriendsOnPartition)) / numFriendsTotal;
    }

    static TShortShortMap getRemainingSpotsInPartitions(TShortSet pidsToSkip, int numUsers, TShortShortMap pToMasterCounts) {
        int numPartitions = pToMasterCounts.size() - pidsToSkip.size();
        int maxUsersPerPartition = numUsers / numPartitions;
        if (numUsers % numPartitions != 0) {
            maxUsersPerPartition++;
        }

        TShortShortMap remainingSpotsInPartitions = new TShortShortHashMap(pToMasterCounts.size()+1);
        for (short pid : pToMasterCounts.keys()) {
            if(pidsToSkip.contains(pid)) {
                continue;
            }
            remainingSpotsInPartitions.put(pid, (short)(maxUsersPerPartition - pToMasterCounts.get(pid)));
        }

        return remainingSpotsInPartitions;
    }

}
