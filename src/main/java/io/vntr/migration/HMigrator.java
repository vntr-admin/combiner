package io.vntr.migration;

/**
 * Created by robertlindquist on 4/24/17.
 */
import io.vntr.Utils;
import io.vntr.repartition.Target;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HMigrator {

    public static Map<Integer, Integer> migrateOffPartition(Integer pid, float gamma, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Integer> actualTargets = new HashMap<>();
        Map<Integer, Integer> userCounts = Utils.getUserCounts(partitions);
        Map<Integer, Integer> uidToPidMap = Utils.getUToMasterMap(partitions);
        NavigableSet<Target> preferredTargets = getPreferredTargets(pid, uidToPidMap, partitions, friendships);

        for(Iterator<Target> iter = preferredTargets.descendingIterator(); iter.hasNext(); ) {
            Target target = iter.next();
            if(!isOverloaded(target.pid, userCounts, gamma, uidToPidMap.size(), partitions.size())) {
                actualTargets.put(target.uid, target.pid);
                userCounts.put(target.pid, userCounts.get(target.pid) + 1);
                iter.remove();
            }
        }

        for(Target target : preferredTargets) {
            Integer newPid = getPartitionWithFewestUsers(pid, userCounts);
            actualTargets.put(target.uid, newPid);
            userCounts.put(newPid, userCounts.get(newPid) + 1);
        }

        return actualTargets;
    }

    static boolean isOverloaded(Integer pid, Map<Integer, Integer> userCounts, float gamma, int numUsers, int numPartitions) {
        float avgUsers = ((float)numUsers) / ((float)numPartitions-1);
        int cutoff = (int) (avgUsers * gamma) + 1;
        return userCounts.get(pid) > cutoff;
    }

    static NavigableSet<Target> getPreferredTargets(Integer pid, Map<Integer, Integer> uidToPidMap, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
        List<Integer> options = new LinkedList<>(partitions.keySet());
        options.remove(pid);
        NavigableSet<Target> preferredTargets = new TreeSet<>();
        for(Integer uid : partitions.get(pid)) {
            Map<Integer, Integer> pToFriendCount = Utils.getPToFriendCount(uid, friendships, uidToPidMap, partitions.keySet());
            int maxFriends = 0;
            Integer maxPid = null;
            for(Integer friendPid : pToFriendCount.keySet()) {
                if(friendPid.equals(pid)) {
                    continue;
                }
                int numFriends = pToFriendCount.get(friendPid);
                if(numFriends > maxFriends) {
                    maxPid = friendPid;
                    maxFriends = numFriends;
                }
            }

            if(maxPid == null) {
                maxPid = ProbabilityUtils.getRandomElement(options);
            }
            Target target = new Target(uid, maxPid, pid, maxFriends);
            preferredTargets.add(target);
        }
        return preferredTargets;
    }

    static Integer getPartitionWithFewestUsers(Integer pid, Map<Integer, Integer> userCounts) {
        int minUsers = Integer.MAX_VALUE;
        Integer minPartition = null;
        for(Integer newPid : userCounts.keySet()) {
            if(newPid.equals(pid)) {
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
