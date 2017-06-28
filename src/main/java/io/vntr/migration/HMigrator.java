package io.vntr.migration;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import io.vntr.repartition.Target;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HMigrator {

    public static TIntIntMap migrateOffPartition(Integer pid, float gamma, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> friendships) {
        TIntIntMap userCounts = getUserCounts(partitions);
        TIntIntMap uidToPidMap = getUToMasterMap(partitions);
        NavigableSet<Target> preferredTargets = getPreferredTargets(pid, uidToPidMap, partitions, friendships);
        TIntIntMap strategy = new TIntIntHashMap(preferredTargets.size()+1);

        for(Iterator<Target> iter = preferredTargets.descendingIterator(); iter.hasNext(); ) {
            Target target = iter.next();
            if(!isOverloaded(target.pid, userCounts, gamma, uidToPidMap.size())) {
                strategy.put(target.uid, target.pid);
                userCounts.put(target.pid, userCounts.get(target.pid) + 1);
                iter.remove();
            }
        }

        WaterFillingPriorityQueue priorityQueue = new WaterFillingPriorityQueue(partitions, new TIntIntHashMap(), pid);

        for(Target target : preferredTargets) {
            int newPid = priorityQueue.getNextPid();
            strategy.put(target.uid, newPid);
        }

        return strategy;
    }

    static boolean isOverloaded(Integer pid, TIntIntMap userCounts, float gamma, int numUsers) {
        float avgUsers = ((float)numUsers) / ((float)userCounts.size()-1);
        int cutoff = (int) (avgUsers * gamma) + 1;
        return userCounts.get(pid) > cutoff;
    }

    static NavigableSet<Target> getPreferredTargets(Integer pid, TIntIntMap uidToPidMap, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> friendships) {
        int[] options = removeUniqueElementFromNonEmptyArray(partitions.keys(), pid);
        NavigableSet<Target> preferredTargets = new TreeSet<>();
        for(TIntIterator iter = partitions.get(pid).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            TIntIntMap pToFriendCount = getPToFriendCount(uid, friendships, uidToPidMap, partitions.keySet());
            int maxFriends = 0;
            Integer maxPid = null;
            for(Integer friendPid : options) {
                int numFriends = pToFriendCount.get(friendPid);
                if(numFriends > maxFriends) {
                    maxPid = friendPid;
                    maxFriends = numFriends;
                }
            }

            if(maxPid == null) {
                maxPid = getKDistinctValuesFromArray(1, options).iterator().next();
            }

            Target target = new Target(uid, maxPid, pid, (float) maxFriends);
            preferredTargets.add(target);
        }
        return preferredTargets;
    }

}
