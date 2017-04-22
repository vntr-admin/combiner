package io.vntr.hermar;

import io.vntr.repartition.Target;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermarMigrator {
    private HermarManager manager;
    private float gamma;

    public HermarMigrator(HermarManager manager, float gamma) {
        this.manager = manager;
        this.gamma = gamma;
    }

    public void migrateOffPartition(Integer pid) {
        NavigableSet<Target> preferredTargets = getPreferredTargets(pid);
        Map<Integer, Integer> actualTargets = new HashMap<>();
        Map<Integer, Integer> userCounts = getUserCounts();

        for(Iterator<Target> iter = preferredTargets.descendingIterator(); iter.hasNext(); ) {
            Target target = iter.next();
            if(!isOverloaded(target.pid, userCounts)) {
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

        for(Integer uid : actualTargets.keySet()) {
            manager.moveUser(uid, actualTargets.get(uid), true);
        }
    }

    private Map<Integer, Integer> getUserCounts() {
        Map<Integer,Set<Integer>> partitionToUserMap = manager.getPartitionToUsers();
        Map<Integer, Integer> map = new HashMap<>();
        for(Integer pid : partitionToUserMap.keySet()) {
            map.put(pid, partitionToUserMap.get(pid).size());
        }
        return map;
    }

    boolean isOverloaded(Integer pid, Map<Integer, Integer> userCounts) {
        float numUsers = manager.getNumUsers();
        float numPartitions = manager.getPids().size() - 1;
        float avgUsers = numUsers / numPartitions;
        int cutoff = (int) (avgUsers * gamma) + 1;
        return userCounts.get(pid) > cutoff;
    }

    NavigableSet<Target> getPreferredTargets(Integer pid) {
        List<Integer> options = new LinkedList<>(manager.getPids());
        options.remove(pid);
        NavigableSet<Target> preferredTargets = new TreeSet<>();
        for(Integer uid : manager.getPartition(pid)) {
            Map<Integer, Integer> pToFriendCount = getPToFriendCount(uid);
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

    Map<Integer, Integer> getPToFriendCount(Integer uid) {
        Map<Integer, Integer> pToFriendCount = new HashMap<>();
        for(Integer pid : manager.getPids()) {
            pToFriendCount.put(pid, 0);
        }
        for(Integer friendId : manager.getUser(uid).getFriendIDs()) {
            Integer pid = manager.getPidForUser(friendId);
            pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
        }
        return pToFriendCount;
    }

    Integer getPartitionWithFewestUsers(Integer pid, Map<Integer, Integer> userCounts) {
        int minUsers = Integer.MAX_VALUE;
        Integer minPartition = null;
        for(Integer newPid : manager.getPids()) {
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
