package io.vntr.hermes;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesManager {
    private NavigableMap<Long, HermesPartition> pMap;
    private Map<Long, Long> uMap;
    private double gamma;


    private static final long defaultStartingPid = 1L;

    public HermesManager(double gamma) {
        this.gamma = gamma;
        pMap = new TreeMap<Long, HermesPartition>();
        uMap = new HashMap<Long, Long>();
    }

    public void addUser(User user) {
        Long initialPid = getInitialPartitionId();
        HermesUser hermesUser = new HermesUser(user.getId(), user.getName(), initialPid, gamma, this);
        getPartitionById(initialPid).addUser(hermesUser);
    }

    public void removeUser(Long userId) {
        getPartitionById(getPartitionIdForUser(userId)).removeUser(userId);
    }

    public void befriend(Long smallerUserId, Long largerUserId) {
        HermesUser smallerUser = getPartitionById(getPartitionIdForUser(smallerUserId)).getUserById(smallerUserId);
        smallerUser.befriend(largerUserId);
        HermesUser largerUser = getPartitionById(getPartitionIdForUser(largerUserId)).getUserById(largerUserId);
        largerUser.befriend(smallerUserId);
    }

    public void unfriend(Long smallerUserId, Long largerUserId) {
        HermesUser smallerUser = getPartitionById(getPartitionIdForUser(smallerUserId)).getUserById(smallerUserId);
        smallerUser.unfriend(largerUserId);
        HermesUser largerUser = getPartitionById(getPartitionIdForUser(largerUserId)).getUserById(largerUserId);
        largerUser.unfriend(smallerUserId);
    }

    public void addPartition() {
        if(pMap.isEmpty()) {
            pMap.put(defaultStartingPid, new HermesPartition(defaultStartingPid, gamma, this));
        }
        else {
            Long nextId = pMap.lastKey() + 1;
            pMap.put(nextId, new HermesPartition(nextId, gamma, this));
        }
    }

    public void removePartition(Long pid) {
        pMap.remove(pid);
    }

    public Set<Long> getAllPartitionIds() {
        return pMap.keySet();
    }

    public HermesPartition getPartitionById(Long pid) { return pMap.get(pid); }
    public Long getPartitionIdForUser(Long uid) { return uMap.get(uid); }

    public void repartition() {
        int k = 3; //TODO: set this intelligently
        for (HermesPartition p : pMap.values()) {
            p.resetLogicalUsers();
        }

        boolean stoppingCondition = false;
        while(!stoppingCondition) {
            boolean changed = false;
            for (HermesPartition p : pMap.values()) {
                Set<Target> targets = p.getCandidates(true, k);
                changed |= !targets.isEmpty();
                for(Target target : targets) {
                    migrateLogically(target);
                }
            }

            updateAggregateWeightInformation();

            for (HermesPartition p : pMap.values()) {
                Set<Target> targets = p.getCandidates(false, k);
                changed |= !targets.isEmpty();
                for(Target target : targets) {
                    migrateLogically(target);
                }
            }

            updateAggregateWeightInformation();

            stoppingCondition = !changed;
        }

        for (HermesPartition p : pMap.values()) {
            p.physicallyMigrateCopy();
        }

        for (HermesPartition p : pMap.values()) {
            p.physicallyMigrateDelete();
        }
    }

    void migrateLogically(Target target) {
        HermesPartition oldPart = getPartitionById(target.oldPartitionId);
        HermesPartition newPart = getPartitionById(target.partitionId);
        HermesUser user = getPartitionById(getPartitionIdForUser(target.userId)).getUserById(target.userId);
        user.setLogicalPid(target.partitionId);
        oldPart.removeLogicalUser(target.userId);
        newPart.addLogicalUser(user.getLogicalUser()); //TODO: is this actually what we want?
    }

    void updateAggregateWeightInformation() {
        Long totalWeight = 0L;
        Map<Long, Long> pToWeight = new HashMap<Long, Long>();
        for (HermesPartition p : pMap.values()) {
            long weight = p.getNumLogicalUsers();
            totalWeight += weight;
            pToWeight.put(p.getId(), weight);
        }

        for (HermesPartition p : pMap.values()) {
            p.updateLogicalUsersPartitionWeights(pToWeight);
            p.updateLogicalUsersTotalWeights(totalWeight);
        }
    }

    Long getInitialPartitionId() {
        Long minId = null;
        int minUsers = Integer.MAX_VALUE;
        for(Long pid : pMap.keySet()) {
            int numUsers = getPartitionById(pid).getNumUsers();
            if(numUsers < minUsers) {
                minUsers = numUsers;
                minId = pid;
            }
        }
        return minId;
    }

    HermesUser getUser(Long uid) {
        return getPartitionById(getPartitionIdForUser(uid)).getUserById(uid);
    }

    public Long getNumUsers() {
        return (long) uMap.size();
    }

    public Long getEdgeCut() {
        long count = 0;
        for(Long uid : uMap.keySet()) {
            LogicalUser user = getUser(uid).getLogicalUser();
            Map<Long, Long> pToFriendCount = user.getpToFriendCount();
            for(Long pid : getAllPartitionIds()) {
                if(!pid.equals(user.getPid()) && pToFriendCount.containsKey(pid)) {
                    count += pToFriendCount.get(pid);
                }

            }
        }
        return count / 2;
    }

    public Map<Long,Set<Long>> getPartitionToUserMap() {
        Map<Long,Set<Long>> map = new HashMap<Long, Set<Long>>();
        for(Long pid : getAllPartitionIds()) {
            map.put(pid, getPartitionById(pid).getPhysicalUserIds());
        }
        return map;
    }

    public void moveUser(Long uid, Long pid) {
        HermesUser user = getUser(uid);
        uMap.put(uid, pid);
        getPartitionById(user.getPhysicalPid()).removeUser(uid);
        getPartitionById(pid).addUser(user);
        user.setPhysicalPid(pid);
        user.setLogicalPid(pid);
    }
}
