package io.vntr.hermes;

import io.vntr.User;

import java.util.*;

import org.apache.log4j.Logger;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesManager {
    final static Logger logger = Logger.getLogger(HermesManager.class);

    private static final String preRepartitionFormatStr  = "#U: %5d | #P: %4d | #F: %7d | Pre-Cut: %7d";
    private static final String postRepartitionFormatStr = "#U: %5d | #P: %4d | #F: %7d | #Iter: %4d | Post-Cut: %7d";
    private static final String iterationFormatStr = "%4d: %7d";

    private static final String repartitionFormatStr = "#U: %5d | #P: %4d | #F: %7d | #Iter: %4d | Pre-Cut: %7d | Post-Cut: %7d";

    private NavigableMap<Integer, HermesPartition> pMap;
    private NavigableMap<Integer, Integer> uMap;
    private float gamma;
    private int k;
    private boolean probabilistic;
    private int maxIterations;
    private float maxIterationToNumUsersRatio;

    private static final int defaultStartingPid = 1;

    public HermesManager(float gamma, boolean probabilistic) {
        this.gamma = gamma;
        this.probabilistic = probabilistic;
        this.pMap = new TreeMap<Integer, HermesPartition>();
        this.uMap = new TreeMap<Integer, Integer>();
        this.maxIterations = Integer.MAX_VALUE;
        maxIterationToNumUsersRatio = 1f;
    }

    public HermesManager(float gamma, int maxIterations) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new TreeMap<Integer, HermesPartition>();
        this.uMap = new TreeMap<Integer, Integer>();
        this.maxIterations = maxIterations;
        maxIterationToNumUsersRatio = 1f;
        this.k=3;
    }

    public HermesManager(float gamma, float maxIterationToNumUsersRatio) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new TreeMap<Integer, HermesPartition>();
        this.uMap = new TreeMap<Integer, Integer>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k=3;
    }

    public HermesManager(float gamma, float maxIterationToNumUsersRatio, int k) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new TreeMap<Integer, HermesPartition>();
        this.uMap = new TreeMap<Integer, Integer>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k = k;
    }

    public int addUser() {
        int newUid = uMap.lastKey() + 1;
        addUser(new User(newUid));
        return newUid;
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
        if(maxIterationToNumUsersRatio != 1f) {
            maxIterations = (int) (Math.ceil(maxIterationToNumUsersRatio * getNumUsers()));
        }
    }

    public void removeUser(Integer userId) {
        Set<Integer> friendIds = new HashSet<Integer>(getUser(userId).getFriendIDs());
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
        for (HermesPartition p : pMap.values()) {
            p.resetLogicalUsers();
        }

//        logger.warn("Original: " + getPartitionToUserMap());
//        logger.warn("Friends: " + getFriendshipMap());

        int numUsers = getNumUsers();
        int numPartitions = pMap.size();
        int numFriendships = 0;
        for(int uid : uMap.keySet()) {
            numFriendships += getUser(uid).getFriendIDs().size();
        }
        int initialCut = getEdgeCut();

        logger.warn(String.format(preRepartitionFormatStr, numUsers, numPartitions, numFriendships, initialCut));

        LinkedList<Integer> lastFive = new LinkedList<Integer>();

        int iteration = 0;
        boolean stoppingCondition = false;
        while(!stoppingCondition && iteration++ < maxIterations) {
            boolean changed = false;
            changed |= performStage(true, k, probabilistic);
            changed |= performStage(false, k, probabilistic);
            stoppingCondition = !changed;
            int edgeCut = getEdgeCut();
            logger.warn(String.format(iterationFormatStr, iteration, edgeCut));
            lastFive.add(edgeCut);
            if(lastFive.size() > 5) {
                lastFive.pop();
                if(lastFive.get(0) == edgeCut && lastFive.get(1) == edgeCut && lastFive.get(2) == edgeCut && lastFive.get(3) == edgeCut) {
                    logger.warn("Five alive!");
                    break;
                }
            }
        }
//        System.out.println("Number of iterations: " + iteration);

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

        int finalCut = getEdgeCut();
//        logger.warn(String.format(repartitionFormatStr, numUsers, numPartitions, numFriendships, iteration, initialCut, finalCut));
        logger.warn(String.format(postRepartitionFormatStr, numUsers, numPartitions, numFriendships, iteration, finalCut));
    }

    boolean performStage(boolean firstStage, int k, boolean probabilistic) {
        boolean changed = false;
        Map<Integer, Set<Target>> stageTargets = new HashMap<Integer, Set<Target>>();
        for (HermesPartition p : pMap.values()) {
            Set<Target> targets = p.getCandidates(firstStage, k, probabilistic);
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
        return uMap.size();
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
        Map<Integer, Set<Integer>> map = new TreeMap<Integer, Set<Integer>>();
        for(Integer pid : getAllPartitionIds()) {
            map.put(pid, new TreeSet<Integer>(getPartitionById(pid).getPhysicalUserIds()));
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

    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new TreeMap<Integer, Set<Integer>>();
        for(Integer uid : uMap.keySet()) {
            friendships.put(uid, new TreeSet<Integer>(getUser(uid).getFriendIDs()));
        }
        return friendships;
    }

    public Collection<Integer> getUserIds() {
        return uMap.keySet();
    }

    public float getGamma() {
        return gamma;
    }

    @Override
    public String toString() {
        return "|gamma:" + gamma + "|probabilistic:" + probabilistic + "|#U:" + getNumUsers() + "|#P:" + pMap.size();
    }
}
