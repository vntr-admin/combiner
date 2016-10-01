package io.vntr.hermes;

import io.vntr.User;

import java.util.*;

import org.apache.log4j.Logger;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesManager {
    final static Logger logger = Logger.getLogger(HermesManager.class);

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
        addUser(hermesUser);
    }

    void addUser(HermesUser user) {
        Long pid = user.getPhysicalPid();
        getPartitionById(pid).addUser(user);
        uMap.put(user.getId(), pid);
    }

    public void removeUser(Long userId) {
        Set<Long> friendIds = getUser(userId).getFriendIDs();
        for(Long friendId : friendIds) {
            unfriend(userId, friendId);
        }
        getPartitionById(getPartitionIdForUser(userId)).removeUser(userId);
        uMap.remove(userId);
    }

    public void befriend(Long smallerUserId, Long largerUserId) {
        getUser(smallerUserId).befriend(largerUserId);
        getUser(largerUserId).befriend(smallerUserId);
    }

    public void unfriend(Long smallerUserId, Long largerUserId) {
        getUser(smallerUserId).unfriend(largerUserId);
        getUser(largerUserId).unfriend(smallerUserId);
    }

    public Long addPartition() {
        Long pid = pMap.isEmpty() ? defaultStartingPid : pMap.lastKey() + 1L;
        addPartition(pid);
        return pid;
    }

    void addPartition(Long id) {
        pMap.put(id, new HermesPartition(id, gamma, this));
    }

    public void removePartition(Long pid) {
        pMap.remove(pid);
    }

    public Set<Long> getAllPartitionIds() {
        return pMap.keySet();
    }

    public HermesPartition getPartitionById(Long pid) {
        return pMap.get(pid);
    }

    public Long getPartitionIdForUser(Long uid) {
        return uMap.get(uid);
    }

    private Map<Long, Set<Long>> getFriendshipMap() {
        Map<Long, Set<Long>> friendshipMap = new HashMap<Long, Set<Long>>();
        for(Long uid : uMap.keySet()) {
            friendshipMap.put(uid, new HashSet<Long>());
        }
        for(Long uid : friendshipMap.keySet()) {
            for(Long friendId : getUser(uid).getFriendIDs())
            {
                if(uid.longValue() < friendId.longValue()) {
                    friendshipMap.get(uid).add(friendId);
                }
            }
        }
        return friendshipMap;
    }

    public void repartition() {
        int k = 3; //TODO: set this intelligently
        for (HermesPartition p : pMap.values()) {
            p.resetLogicalUsers();
        }

        logger.warn("Original: " + getPartitionToUserMap());
        logger.warn("Friends: " + getFriendshipMap());

        int iteration = 1;
        boolean stoppingCondition = false;
        while(!stoppingCondition) {
            boolean changed = false;
            changed |= performStage(true, k);
            changed |= performStage(false, k);
            stoppingCondition = !changed;
            iteration++;
        }
        System.out.println("Number of iterations: " + iteration);

        Map<Long, Long> usersWhoMoved = new HashMap<Long, Long>();
        for (HermesPartition p : pMap.values()) {
            Set<Long> moved = p.physicallyMigrateCopy();
            for(Long uid : moved) {
                usersWhoMoved.put(uid, p.getId());
            }
        }

        for (HermesPartition p : pMap.values()) {
            p.physicallyMigrateDelete();
        }

        uMap.putAll(usersWhoMoved);
    }

    boolean performStage(boolean firstStage, int k) {
        boolean changed = false;
        Map<Long, Set<Target>> stageTargets = new HashMap<Long, Set<Target>>();
        for (HermesPartition p : pMap.values()) {
            Set<Target> targets = p.getCandidates(firstStage, k);
            stageTargets.put(p.getId(), targets);
            changed |= !targets.isEmpty();
        }

        for(Long pid : pMap.keySet()) {
            for(Target target : stageTargets.get(pid)) {
                migrateLogically(target);
            }
        }

        updateAggregateWeightInformation();
        return changed;
    }

    void migrateLogically(Target target) {
        HermesPartition oldPart = getPartitionById(target.oldPartitionId);
        HermesPartition newPart = getPartitionById(target.partitionId);
        HermesUser user = getUser(target.userId);
        user.setLogicalPid(target.partitionId);
        oldPart.removeLogicalUser(target.userId);
        newPart.addLogicalUser(user.getLogicalUser(false)); //TODO: is this actually what we want?
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
            LogicalUser user = getUser(uid).getLogicalUser(true);
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

    Map<Long,Set<Long>> getPartitionToLogicalUserMap() {
        Map<Long,Set<Long>> map = new HashMap<Long, Set<Long>>();
        for(Long pid : getAllPartitionIds()) {
            map.put(pid, getPartitionById(pid).getLogicalUserIds());
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
