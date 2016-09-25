package io.vntr.hermes;

import java.util.*;

/**
 * Created by robertlindquist on 9/23/16.
 */
public class HermesMigrator {
    private HermesManager manager;
    private double gamma;

    public HermesMigrator(HermesManager manager, double gamma) {
        this.manager = manager;
        this.gamma = gamma;
    }

    public void migrateOffPartition(Long pid) {
        NavigableSet<Target> preferredTargets = getPreferredTargets(pid);
        Map<Long, Long> actualTargets = new HashMap<Long, Long>();
        Map<Long, Integer> userCounts = getUserCounts();

        for(Iterator<Target> iter = preferredTargets.descendingIterator(); iter.hasNext(); ) {
            Target target = iter.next();
            if(!isOverloaded(target.partitionId, userCounts)) {
                actualTargets.put(target.userId, target.partitionId);
                userCounts.put(target.partitionId, userCounts.get(target.partitionId) + 1);
                iter.remove();
            }
        }

        for(Target target : preferredTargets) {
            Long newPid = getPartitionWithFewestUsers(pid, userCounts);
            actualTargets.put(target.userId, newPid);
            userCounts.put(newPid, userCounts.get(newPid) + 1);
        }

        for(Long uid : actualTargets.keySet()) {
            manager.moveUser(uid, actualTargets.get(uid));
        }
    }

    private Map<Long, Integer> getUserCounts() {
        Map<Long,Set<Long>> partitionToUserMap = manager.getPartitionToUserMap();
        Map<Long, Integer> map = new HashMap<Long, Integer>();
        for(Long pid : partitionToUserMap.keySet()) {
            map.put(pid, partitionToUserMap.get(pid).size());
        }
        return map;
    }

    boolean isOverloaded(Long pid, Map<Long, Integer> userCounts) {
        double numUsers = manager.getNumUsers();
        double numPartitions = manager.getAllPartitionIds().size() - 1;
        double avgUsers = numUsers / numPartitions;
        int cutoff = (int) (avgUsers * gamma) + 1;
        return userCounts.get(pid) > cutoff;
    }

    NavigableSet<Target> getPreferredTargets(Long pid) {
        NavigableSet<Target> preferredTargets = new TreeSet<Target>();
        for(Long uid : manager.getPartitionById(pid).getPhysicalUserIds()) {
            LogicalUser user = manager.getUser(uid).getLogicalUser(true);
            long maxFriends = 0L;
            Long maxPid = null;
            for(Long friendPid : user.getpToFriendCount().keySet()) {
                long numFriends = user.getpToFriendCount().get(friendPid);
                if(numFriends > maxFriends) {
                    maxPid = friendPid;
                    maxFriends = numFriends;
                }
            }

            Target target = new Target(uid, maxPid, pid, (int) maxFriends);
            preferredTargets.add(target);
        }
        return preferredTargets;
    }

    Long getPartitionWithFewestUsers(Long pid, Map<Long, Integer> userCounts) {
        int minUsers = Integer.MAX_VALUE;
        Long minPartition = null;
        for(Long newPid : manager.getAllPartitionIds()) {
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
