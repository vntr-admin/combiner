package io.vntr.hermes;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesManager {
    private Map<Integer, Set<Integer>> pMap;
    private Map<Integer, User> uidToUserMap;
    private float gamma;
    private int k;
    private boolean probabilistic;
    private int maxIterations;
    private float maxIterationToNumUsersRatio;

    private long migrationTally;
    private long logicalMigrationTally;
    private double logicalMigrationRatio = 0;

    private int nextPid = 1;
    private int nextUid = 1;

    private HermesRepartitioner repartitioner;

    public HermesManager(float gamma, boolean probabilistic) {
        this.gamma = gamma;
        this.probabilistic = probabilistic;
        this.pMap = new HashMap<>();
        this.uidToUserMap = new HashMap<>();
        repartitioner = new HermesRepartitioner(this);
        this.maxIterations = 100;
        maxIterationToNumUsersRatio = 1f;
    }

    public HermesManager(float gamma, int maxIterations) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uidToUserMap = new HashMap<>();
        repartitioner = new HermesRepartitioner(this);
        this.maxIterations = maxIterations;
        maxIterationToNumUsersRatio = 1f;
        this.k=3;
    }

    public HermesManager(float gamma, float maxIterationToNumUsersRatio) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uidToUserMap = new HashMap<>();
        repartitioner = new HermesRepartitioner(this);
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k=3;
    }

    public HermesManager(float gamma, float maxIterationToNumUsersRatio, int k) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uidToUserMap = new HashMap<>();
        repartitioner = new HermesRepartitioner(this);
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k = k;
    }

    public HermesManager(float gamma, float maxIterationToNumUsersRatio, int k, double logicalMigrationRatio) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uidToUserMap = new HashMap<>();
        repartitioner = new HermesRepartitioner(this);
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
        if(user.getBasePid() == null) {
            user.setBasePid(getInitialPartitionId());
        }
        Integer pid = user.getBasePid();
        uidToUserMap.put(user.getId(), user);
        getPartitionById(pid).add(user.getId());
        if(maxIterationToNumUsersRatio != 1f) {
            maxIterations = (int) (Math.ceil(maxIterationToNumUsersRatio * getNumUsers()));
        }
        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }
    }

    public void removeUser(Integer uid) {
        Set<Integer> friendIds = new HashSet<>(getUser(uid).getFriendIDs());
        for(Integer friendId : friendIds) {
            unfriend(uid, friendId);
        }
        getPartitionById(getPartitionIdForUser(uid)).remove(uid);
        uidToUserMap.remove(uid);
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
        pMap.put(id, new HashSet<Integer>());
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

    public Set<Integer> getPartitionById(Integer pid) {
        return pMap.get(pid);
    }

    public Integer getPartitionIdForUser(Integer uid) {
        return uidToUserMap.get(uid).getBasePid();
    }

    public void repartition() {
        repartitioner.repartition(k, maxIterations);
    }

    Integer getInitialPartitionId() {
        Integer minId = null;
        int minUsers = Integer.MAX_VALUE;
        for(Integer pid : pMap.keySet()) {
            int numUsers = getPartitionById(pid).size();
            if(numUsers < minUsers) {
                minUsers = numUsers;
                minId = pid;
            }
        }
        return minId;
    }

    User getUser(Integer uid) {
        return uidToUserMap.get(uid);
    }

    public Integer getNumUsers() {
        return uidToUserMap.size();
    }

    public Integer getEdgeCut() {
        int count = 0;
        for(int uid : uidToUserMap.keySet()) {
            User user = getUser(uid);
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
            map.put(pid, new HashSet<>(getPartitionById(pid)));
        }
        return map;
    }

    public void moveUser(Integer uid, Integer pid) {
        User user = getUser(uid);
        getPartitionById(user.getBasePid()).remove(uid);
        getPartitionById(pid).add(uid);
        user.setBasePid(pid);
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : uidToUserMap.keySet()) {
            friendships.put(uid, new TreeSet<>(getUser(uid).getFriendIDs()));
        }
        return friendships;
    }

    public Collection<Integer> getUserIds() {
        return uidToUserMap.keySet();
    }

    public float getGamma() {
        return gamma;
    }

    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * logicalMigrationTally);
    }

    void increaseTally(int amount) {
        migrationTally += amount;
    }

    void increaseTallyLogical(int amount) {
        logicalMigrationTally += amount;
    }


    @Override
    public String toString() {
        return "|gamma:" + gamma + "|probabilistic:" + probabilistic + "|#U:" + getNumUsers() + "|#P:" + pMap.size();
    }

    void checkValidity() {
        for(Integer uid : uidToUserMap.keySet()) {
            Integer observedMasterPid = null;
            for(Integer pid : pMap.keySet()) {
                if(pMap.get(pid).contains(uid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot be in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must be in some partition");
            }
            if(!observedMasterPid.equals(getUser(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }
            if(!observedMasterPid.equals(uidToUserMap.get(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's PID and system's");
            }
        }
    }
}
