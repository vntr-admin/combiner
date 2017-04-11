package io.vntr.hermar;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermarManager {
    private Map<Integer, HermarPartition> pMap;
    private Map<Integer, Integer> uMap;
    private Map<Integer, Integer> uMapLogical;
    private float gamma;
    private int k;
    private boolean probabilistic;
    private int maxIterations;
    private float maxIterationToNumUsersRatio;

    private long migrationTally;
    private long migrationTallyLogical;
    private double logicalMigrationRatio = 0;

    private int nextPid = 1;
    private int nextUid = 1;


    public HermarManager(float gamma, boolean probabilistic) {
        this.gamma = gamma;
        this.probabilistic = probabilistic;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.uMapLogical = new HashMap<>();
        this.maxIterations = Integer.MAX_VALUE;
        maxIterationToNumUsersRatio = 1f;
    }

    public HermarManager(float gamma, int maxIterations) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.uMapLogical = new HashMap<>();
        this.maxIterations = maxIterations;
        maxIterationToNumUsersRatio = 1f;
        this.k=3;
    }

    public HermarManager(float gamma, float maxIterationToNumUsersRatio) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.uMapLogical = new HashMap<>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k=3;
    }

    public HermarManager(float gamma, float maxIterationToNumUsersRatio, int k) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.uMapLogical = new HashMap<>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k = k;
    }

    public HermarManager(float gamma, float maxIterationToNumUsersRatio, int k, double logicalMigrationRatio) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.uMapLogical = new HashMap<>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k = k;
        this.logicalMigrationRatio = logicalMigrationRatio;
    }

    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer initialPid = getInitialPartitionId();
        int uid = user.getId();
        HermarUser hermarUser = new HermarUser(uid, initialPid, gamma, this);
        addUser(hermarUser);
        if(uid >= nextUid) {
            nextUid = uid + 1;
        }
    }

    void addUser(HermarUser user) {
        Integer pid = user.getPhysicalPid();
        getPartitionById(pid).addUser(user);
        uMap.put(user.getId(), pid);
        uMapLogical.put(user.getId(), pid);
        if(maxIterationToNumUsersRatio != 1f) {
            maxIterations = (int) (Math.ceil(maxIterationToNumUsersRatio * getNumUsers()));
        }
    }

    public void removeUser(Integer userId) {
        Set<Integer> friendIds = new HashSet<>(getUser(userId).getFriendIDs());
        for(Integer friendId : friendIds) {
            unfriend(userId, friendId);
        }
        getPartitionById(getPartitionIdForUser(userId)).removeUser(userId);
        uMap.remove(userId);
        uMapLogical.remove(userId);
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
        int pid = nextPid;
        addPartition(pid);
        return pid;
    }

    void addPartition(Integer id) {
        pMap.put(id, new HermarPartition(id, gamma, this));
        if(id >= nextPid) {
            nextPid = id + 1;
        }
    }

    public void removePartition(Integer pid) {
        pMap.remove(pid);
    }

    public Set<Integer> getAllPartitionIds() {
        return pMap.keySet();
    }

    public HermarPartition getPartitionById(Integer pid) {
        return pMap.get(pid);
    }

    public Integer getPartitionIdForUser(Integer uid) {
        return uMap.get(uid);
    }

    public Integer getLogicalPartitionIdForUser(Integer uid) {
        return uMapLogical.get(uid);
    }

    private Map<Integer, Set<Integer>> getFriendshipMap() {
        Map<Integer, Set<Integer>> friendshipMap = new HashMap<>();
        for(Integer uid : uMap.keySet()) {
            friendshipMap.put(uid, new HashSet<Integer>());
        }
        for(Integer uid : friendshipMap.keySet()) {
            for(Integer friendId : getUser(uid).getFriendIDs())
            {
                if(uid < friendId) {
                    friendshipMap.get(uid).add(friendId);
                }
            }
        }
        return friendshipMap;
    }

    public void repartition() {
        for (HermarPartition p : pMap.values()) {
            p.resetLogicalUsers();
        }

        LinkedList<Integer> lastFive = new LinkedList<>();

        int iteration = 0;
        boolean stoppingCondition = false;
        while(!stoppingCondition && iteration++ < maxIterations) {
            boolean changed;
            changed  = performStage(true, k, probabilistic);
            changed |= performStage(false, k, probabilistic);
            stoppingCondition = !changed;
            int edgeCut = getEdgeCut();
            lastFive.add(edgeCut);
            if(lastFive.size() > 5) {
                lastFive.pop();
                if(lastFive.get(0) == edgeCut && lastFive.get(1) == edgeCut && lastFive.get(2) == edgeCut && lastFive.get(3) == edgeCut) {
                    break;
                }
            }
        }
        Map<Integer, Integer> usersWhoMoved = new HashMap<>();
        for (HermarPartition p : pMap.values()) {
            Set<Integer> moved = p.physicallyMigrateCopy();
            for(Integer uid : moved) {
                usersWhoMoved.put(uid, p.getId());
            }
        }

        for (HermarPartition p : pMap.values()) {
            p.physicallyMigrateDelete();
        }

        uMap.putAll(usersWhoMoved);
        uMapLogical.putAll(usersWhoMoved);
        increaseTally(usersWhoMoved.size());
    }

    boolean performStage(boolean firstStage, int k, boolean probabilistic) {
        boolean changed = false;
        Map<Integer, Set<Target>> stageTargets = new HashMap<>();
        for (HermarPartition p : pMap.values()) {
            Set<Target> targets = p.getCandidates(firstStage, k, probabilistic);
            stageTargets.put(p.getId(), targets);
            changed |= !targets.isEmpty();
        }

        //TODO: should I include logical migrations in the migration tally?
        for(Integer pid : pMap.keySet()) {
            for(Target target : stageTargets.get(pid)) {
                increaseTallyLogical(1);
                migrateLogically(target);
            }
        }

        updateLogicalUsers();
        return changed;
    }

    void migrateLogically(Target target) {
        HermarPartition oldPart = getPartitionById(target.oldPartitionId);
        HermarPartition newPart = getPartitionById(target.partitionId);
        HermarUser user = getUser(target.userId);
        user.setLogicalPid(target.partitionId);
        oldPart.removeLogicalUser(target.userId);
        newPart.addLogicalUser(user.getLogicalUser(false)); //TODO: is this actually what we want?
    }

    void updateLogicalUsers() {
        Integer totalWeight = 0;
        Map<Integer, Integer> pToWeight = new HashMap<>();
        for (HermarPartition p : pMap.values()) {
            int weight = p.getNumLogicalUsers();
            totalWeight += weight;
            pToWeight.put(p.getId(), weight);
        }

        for (HermarPartition p : pMap.values()) {
            p.updateLogicalUsersPartitionWeights(pToWeight);
            p.updateLogicalUsersTotalWeights(totalWeight);
        }

        for(HermarPartition p : pMap.values()) {
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

    HermarUser getUser(Integer uid) {
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
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for(Integer pid : getAllPartitionIds()) {
            map.put(pid, new TreeSet<>(getPartitionById(pid).getPhysicalUserIds()));
        }
        return map;
    }

    Map<Integer,Set<Integer>> getPartitionToLogicalUserMap() {
        Map<Integer,Set<Integer>> map = new HashMap<>();
        for(Integer pid : getAllPartitionIds()) {
            map.put(pid, getPartitionById(pid).getLogicalUserIds());
        }
        return map;
    }

    public void moveUser(Integer uid, Integer pid) {
        HermarUser user = getUser(uid);
        uMap.put(uid, pid);
        uMapLogical.put(uid, pid);
        getPartitionById(user.getPhysicalPid()).removeUser(uid);
        getPartitionById(pid).addUser(user);
        user.setPhysicalPid(pid);
        user.setLogicalPid(pid);
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : uMap.keySet()) {
            friendships.put(uid, new TreeSet<>(getUser(uid).getFriendIDs()));
        }
        return friendships;
    }

    public Collection<Integer> getUserIds() {
        return uMap.keySet();
    }

    public float getGamma() {
        return gamma;
    }

    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * migrationTallyLogical);
    }

    void increaseTally(int amount) {
        migrationTally += amount;
    }

    void increaseTallyLogical(int amount) { migrationTallyLogical += amount; }


    @Override
    public String toString() {
        return "|gamma:" + gamma + "|probabilistic:" + probabilistic + "|#U:" + getNumUsers() + "|#P:" + pMap.size();
    }

    void updateLogicalPidCache(Integer id, Integer logicalPid) {
        uMapLogical.put(id, logicalPid);
    }

    void checkValidity() {
        for(Integer uid : uMap.keySet()) {
            Integer observedMasterPid = null;
            for(Integer pid : pMap.keySet()) {
                if(pMap.get(pid).getPhysicalUserIds().contains(uid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot be in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must be in some partition");
            }
            if(!observedMasterPid.equals(uMap.get(uid))) {
                throw new RuntimeException("Mismatch between uMap's location of user and partition's");
            }
            if(!observedMasterPid.equals(getUser(uid).getPhysicalPid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }

            //TODO: should we check the logical partitions?
        }
    }
}
