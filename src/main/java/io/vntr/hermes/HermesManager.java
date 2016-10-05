package io.vntr.hermes;

import io.vntr.User;

import java.util.*;

import org.apache.log4j.Logger;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesManager {
    final static Logger logger = Logger.getLogger(HermesManager.class);

    private NavigableMap<Integer, HermesPartition> pMap;
    private Map<Integer, Integer> uMap;
    private double gamma;

    private static final int defaultStartingPid = 1;

    public HermesManager(double gamma) {
        this.gamma = gamma;
        pMap = new TreeMap<Integer, HermesPartition>();
        uMap = new HashMap<Integer, Integer>();
    }

    public void addUser(User user) {
        Integer initialPid = getInitialPartitionId();
        HermesUser hermesUser = new HermesUser(user.getId(), initialPid, gamma, this);
        addUser(hermesUser);
    }

    void addUser(HermesUser user) {
        Integer pid = user.getPhysicalPid();
        getPartitionById(pid).addUser(user);
        uMap.put(user.getId(), pid);
    }

    public void removeUser(Integer userId) {
        Set<Integer> friendIds = getUser(userId).getFriendIDs();
        for(Integer friendId : friendIds) {
            unfriend(userId, friendId);
        }
        getPartitionById(getPartitionIdForUser(userId)).removeUser(userId);
        uMap.remove(userId);
    }

    public void befriend(Integer smallerUserId, Integer largerUserId) {
        getUser(smallerUserId).befriend(largerUserId);
        getUser(largerUserId).befriend(smallerUserId);
    }

    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        getUser(smallerUserId).unfriend(largerUserId);
        getUser(largerUserId).unfriend(smallerUserId);
    }

    public Integer addPartition() {
        Integer pid = pMap.isEmpty() ? defaultStartingPid : pMap.lastKey() + 1;
        addPartition(pid);
        return pid;
    }

    void addPartition(Integer id) {
        pMap.put(id, new HermesPartition(id, gamma, this));
    }

    public void removePartition(Integer pid) {
        pMap.remove(pid);
    }

    public Set<Integer> getAllPartitionIds() {
        return pMap.keySet();
    }

    public HermesPartition getPartitionById(Integer pid) {
        return pMap.get(pid);
    }

    public Integer getPartitionIdForUser(Integer uid) {
        return uMap.get(uid);
    }

    private Map<Integer, Set<Integer>> getFriendshipMap() {
        Map<Integer, Set<Integer>> friendshipMap = new HashMap<Integer, Set<Integer>>();
        for(Integer uid : uMap.keySet()) {
            friendshipMap.put(uid, new HashSet<Integer>());
        }
        for(Integer uid : friendshipMap.keySet()) {
            for(Integer friendId : getUser(uid).getFriendIDs())
            {
                if(uid.intValue() < friendId.intValue()) {
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

        Map<Integer, Integer> usersWhoMoved = new HashMap<Integer, Integer>();
        for (HermesPartition p : pMap.values()) {
            Set<Integer> moved = p.physicallyMigrateCopy();
            for(Integer uid : moved) {
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
        Map<Integer, Set<Target>> stageTargets = new HashMap<Integer, Set<Target>>();
        for (HermesPartition p : pMap.values()) {
            Set<Target> targets = p.getCandidates(firstStage, k);
            stageTargets.put(p.getId(), targets);
            changed |= !targets.isEmpty();
        }

        for(Integer pid : pMap.keySet()) {
            for(Target target : stageTargets.get(pid)) {
                migrateLogically(target);
            }
        }

        updateLogicalUsers();
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

    void updateLogicalUsers() {
        Integer totalWeight = 0;
        Map<Integer, Integer> pToWeight = new HashMap<Integer, Integer>();
        for (HermesPartition p : pMap.values()) {
            int weight = p.getNumLogicalUsers();
            totalWeight += weight;
            pToWeight.put(p.getId(), weight);
        }

        for (HermesPartition p : pMap.values()) {
            p.updateLogicalUsersPartitionWeights(pToWeight);
            p.updateLogicalUsersTotalWeights(totalWeight);
        }

        for(HermesPartition p : pMap.values()) {
            for(Integer logicalUid : p.getLogicalUserIds()) {
                Map<Integer, Integer> updatedFriendCounts = getUser(logicalUid).getPToFriendCount();
                p.updateLogicalUserFriendCounts(logicalUid, updatedFriendCounts);
            }
        }
    }

    Integer getInitialPartitionId() {
        Integer minId = null;
        int minUsers = Integer.MAX_VALUE;
        for(Integer pid : pMap.keySet()) {
            int numUsers = getPartitionById(pid).getNumUsers();
            if(numUsers < minUsers) {
                minUsers = numUsers;
                minId = pid;
            }
        }
        return minId;
    }

    HermesUser getUser(Integer uid) {
        return getPartitionById(getPartitionIdForUser(uid)).getUserById(uid);
    }

    public Integer getNumUsers() {
        return (int) uMap.size();
    }

    public Integer getEdgeCut() {
        int count = 0;
        for(Integer uid : uMap.keySet()) {
            LogicalUser user = getUser(uid).getLogicalUser(true);
            Map<Integer, Integer> pToFriendCount = user.getpToFriendCount();
            for(Integer pid : getAllPartitionIds()) {
                if(!pid.equals(user.getPid()) && pToFriendCount.containsKey(pid)) {
                    count += pToFriendCount.get(pid);
                }

            }
        }
        return count / 2;
    }

    public Map<Integer,Set<Integer>> getPartitionToUserMap() {
        Map<Integer,Set<Integer>> map = new HashMap<Integer, Set<Integer>>();
        for(Integer pid : getAllPartitionIds()) {
            map.put(pid, getPartitionById(pid).getPhysicalUserIds());
        }
        return map;
    }

    Map<Integer,Set<Integer>> getPartitionToLogicalUserMap() {
        Map<Integer,Set<Integer>> map = new HashMap<Integer, Set<Integer>>();
        for(Integer pid : getAllPartitionIds()) {
            map.put(pid, getPartitionById(pid).getLogicalUserIds());
        }
        return map;
    }

    public void moveUser(Integer uid, Integer pid) {
        HermesUser user = getUser(uid);
        uMap.put(uid, pid);
        getPartitionById(user.getPhysicalPid()).removeUser(uid);
        getPartitionById(pid).addUser(user);
        user.setPhysicalPid(pid);
        user.setLogicalPid(pid);
    }
}
