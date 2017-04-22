package io.vntr.hermar;

import io.vntr.RepUser;
import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermarManager {
    private Map<Integer, HermarPartition> pMap;
    private Map<Integer, Integer> uMap;
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

    private HermarRepartitioner repartitioner;

    public HermarManager(float gamma, boolean probabilistic) {
        this.gamma = gamma;
        this.probabilistic = probabilistic;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterations = 100;
        maxIterationToNumUsersRatio = 1f;
        repartitioner = new HermarRepartitioner(this);
    }

    public HermarManager(float gamma, int maxIterations) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterations = maxIterations;
        maxIterationToNumUsersRatio = 1f;
        this.k=3;
        repartitioner = new HermarRepartitioner(this);
    }

    public HermarManager(float gamma, float maxIterationToNumUsersRatio) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k=3;
        repartitioner = new HermarRepartitioner(this);
    }

    public HermarManager(float gamma, float maxIterationToNumUsersRatio, int k) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k = k;
        repartitioner = new HermarRepartitioner(this);
    }

    public HermarManager(float gamma, float maxIterationToNumUsersRatio, int k, double logicalMigrationRatio) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k = k;
        this.logicalMigrationRatio = logicalMigrationRatio;
        repartitioner = new HermarRepartitioner(this);
    }

    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer initialPid = getInitialPartitionId();
        int uid = user.getId();
        RepUser RepUser = new RepUser(uid, initialPid);
        addUser(RepUser);
    }

    void addUser(RepUser user) {
        Integer pid = user.getBasePid();
        getPartitionById(pid).addUser(user);
        uMap.put(user.getId(), pid);
        if(maxIterationToNumUsersRatio != 1f) {
            maxIterations = (int) (Math.ceil(maxIterationToNumUsersRatio * getNumUsers()));
        }
        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }

    }

    public void removeUser(Integer userId) {
        Set<Integer> friendIds = new HashSet<>(getUser(userId).getFriendIDs());
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
        int pid = nextPid;
        addPartition(pid);
        return pid;
    }

    void addPartition(Integer id) {
        pMap.put(id, new HermarPartition(id));
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

    public void repartition() {
        repartitioner.repartition(k, maxIterations);
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

    RepUser getUser(Integer uid) {
        return getPartitionById(getPartitionIdForUser(uid)).getUserById(uid);
    }

    public Integer getNumUsers() {
        return uMap.size();
    }

    public Integer getEdgeCut() {
        int count = 0;
        for(int uid : uMap.keySet()) {
            RepUser user = getUser(uid);
            Integer userPid = user.getBasePid();

            for(int friendId : user.getFriendIDs()) {
                Integer friendPid = getUser(friendId).getBasePid();
                if(userPid < friendPid) {
                    count++;
                }
            }
        }
        return count;
    }

    public Map<Integer,Set<Integer>> getPartitionToUserMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for(Integer pid : getAllPartitionIds()) {
            map.put(pid, new TreeSet<>(getPartitionById(pid).getPhysicalUserIds()));
        }
        return map;
    }

    public void moveUser(Integer uid, Integer pid) {
        RepUser user = getUser(uid);
        uMap.put(uid, pid);
        getPartitionById(user.getBasePid()).removeUser(uid);
        getPartitionById(pid).addUser(user);
        user.setBasePid(pid);
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
            if(!observedMasterPid.equals(getUser(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }

            //TODO: should we check the logical partitions?
        }
    }
}
