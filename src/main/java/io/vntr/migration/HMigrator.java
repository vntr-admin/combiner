package io.vntr.migration;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.set.TShortSet;
import io.vntr.repartition.Target;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HMigrator {

    public static TShortShortMap migrateOffPartition(short pid, float gamma, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> friendships) {
        TShortShortMap userCounts = getUserCounts(partitions);
        TShortShortMap uidToPidMap = getUToMasterMap(partitions);
        NavigableSet<Target> preferredTargets = getPreferredTargets(pid, uidToPidMap, partitions, friendships);
        TShortShortMap actualTargets = new TShortShortHashMap(preferredTargets.size()+1);

        for(Iterator<Target> iter = preferredTargets.descendingIterator(); iter.hasNext(); ) {
            Target target = iter.next();
            if(!isOverloaded(target.pid, userCounts, gamma, uidToPidMap.size())) {
                actualTargets.put(target.uid, target.pid);
                userCounts.put(target.pid, userCounts.get((short)(target.pid + 1)));
                iter.remove();
            }
        }

        for(Target target : preferredTargets) {
            short newPid = getPartitionWithFewestUsers(pid, userCounts);
            actualTargets.put(target.uid, newPid);
            userCounts.put(newPid, (short)(userCounts.get(newPid) + 1));
        }

        return actualTargets;
    }

    static boolean isOverloaded(short pid, TShortShortMap userCounts, float gamma, int numUsers) {
        float avgUsers = ((float)numUsers) / ((float)userCounts.size()-1);
        int cutoff = (int) (avgUsers * gamma) + 1;
        return userCounts.get(pid) > cutoff;
    }

    static NavigableSet<Target> getPreferredTargets(short pid, TShortShortMap uidToPidMap, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> friendships) {
        short[] options = removeUniqueElementFromNonEmptyArray(partitions.keys(), pid);
        NavigableSet<Target> preferredTargets = new TreeSet<>();
        for(TShortIterator iter = partitions.get(pid).iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            TShortShortMap pToFriendCount = getPToFriendCount(uid, friendships, uidToPidMap, partitions.keySet());
            int maxFriends = 0;
            Short maxPid = null;
            for(short friendPid : options) {
                int numFriends = pToFriendCount.get(friendPid);
                if(numFriends > maxFriends) {
                    maxPid = friendPid;
                    maxFriends = numFriends;
                }
            }

            if(maxPid == null) {
                maxPid = getKDistinctValuesFromArray((short) 1, options).iterator().next();
            }

            Target target = new Target(uid, maxPid, pid, (float) maxFriends);
            preferredTargets.add(target);
        }
        return preferredTargets;
    }

    static Short getPartitionWithFewestUsers(short pid, TShortShortMap userCounts) {
        int minUsers = Integer.MAX_VALUE;
        Short minPartition = null;
        for(short newPid : userCounts.keys()) {
            if(newPid == pid) {
                continue;
            }
            int numUsers = userCounts.get(newPid);
            if(numUsers < minUsers) {
                minPartition = newPid;
                minUsers = numUsers;
            }
        }
        return minPartition;
    }

}
