package io.vntr.hermes;

import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesMigrator {
    private HermesManager manager;
    private float gamma;

    public HermesMigrator(HermesManager manager, float gamma) {
        this.manager = manager;
        this.gamma = gamma;
    }

    public void migrateOffPartition(Integer pid) {
        NavigableSet<Target> preferredTargets = getPreferredTargets(pid);
        Map<Integer, Integer> actualTargets = new HashMap<>();
        Map<Integer, Integer> userCounts = getUserCounts();

        for(Iterator<Target> iter = preferredTargets.descendingIterator(); iter.hasNext(); ) {
            Target target = iter.next();
            if(!isOverloaded(target.partitionId, userCounts)) {
                actualTargets.put(target.userId, target.partitionId);
                userCounts.put(target.partitionId, userCounts.get(target.partitionId) + 1);
                iter.remove();
            }
        }

        for(Target target : preferredTargets) {
            Integer newPid = getPartitionWithFewestUsers(pid, userCounts);
            actualTargets.put(target.userId, newPid);
            userCounts.put(newPid, userCounts.get(newPid) + 1);
        }

        for(Integer uid : actualTargets.keySet()) {
            manager.moveUser(uid, actualTargets.get(uid));
        }
    }

    private Map<Integer, Integer> getUserCounts() {
        Map<Integer,Set<Integer>> partitionToUserMap = manager.getPartitionToUserMap();
        Map<Integer, Integer> map = new HashMap<>();
        for(Integer pid : partitionToUserMap.keySet()) {
            map.put(pid, partitionToUserMap.get(pid).size());
        }
        return map;
    }

    boolean isOverloaded(Integer pid, Map<Integer, Integer> userCounts) {
        float numUsers = manager.getNumUsers();
        float numPartitions = manager.getAllPartitionIds().size() - 1;
        float avgUsers = numUsers / numPartitions;
        int cutoff = (int) (avgUsers * gamma) + 1;
        return userCounts.get(pid) > cutoff;
    }

    NavigableSet<Target> getPreferredTargets(Integer pid) {
        List<Integer> options = new LinkedList<>(manager.getAllPartitionIds());
        options.remove(pid);
        NavigableSet<Target> preferredTargets = new TreeSet<>();
        for(Integer uid : manager.getPartitionById(pid).getPhysicalUserIds()) {
            LogicalUser user = manager.getUser(uid).getLogicalUser(true);
            int maxFriends = 0;
            Integer maxPid = null;
            for(Integer friendPid : user.getpToFriendCount().keySet()) {
                if(friendPid.equals(pid)) {
                    continue;
                }
                int numFriends = user.getpToFriendCount().get(friendPid);
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

    Integer getPartitionWithFewestUsers(Integer pid, Map<Integer, Integer> userCounts) {
        int minUsers = Integer.MAX_VALUE;
        Integer minPartition = null;
        for(Integer newPid : manager.getAllPartitionIds()) {
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
