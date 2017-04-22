package io.vntr.hermes;

import io.vntr.INoRepManager;
import io.vntr.User;
import io.vntr.repartition.HRepartitioner;
import io.vntr.repartition.Results;

import java.util.*;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesManager implements INoRepManager {
    private Map<Integer, Set<Integer>> pMap;
    private Map<Integer, User> uMap;
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

    public HermesManager(float gamma, boolean probabilistic) {
        this.gamma = gamma;
        this.probabilistic = probabilistic;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterations = 100;
        maxIterationToNumUsersRatio = 1f;
    }

    public HermesManager(float gamma, int maxIterations) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterations = maxIterations;
        maxIterationToNumUsersRatio = 1f;
        this.k=3;
    }

    public HermesManager(float gamma, float maxIterationToNumUsersRatio) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k=3;
    }

    public HermesManager(float gamma, float maxIterationToNumUsersRatio, int k) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k = k;
    }

    public HermesManager(float gamma, float maxIterationToNumUsersRatio, int k, double logicalMigrationRatio) {
        this.gamma = gamma;
        this.probabilistic = false;
        this.pMap = new HashMap<>();
        this.uMap = new HashMap<>();
        this.maxIterationToNumUsersRatio = maxIterationToNumUsersRatio;
        this.maxIterations = (int) (Math.ceil(this.maxIterationToNumUsersRatio * getNumUsers()));
        this.k = k;
        this.logicalMigrationRatio = logicalMigrationRatio;
    }

    @Override
    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    @Override
    public void addUser(User user) {
        if(user.getBasePid() == null) {
            user.setBasePid(getInitialPartitionId());
        }
        Integer pid = user.getBasePid();
        uMap.put(user.getId(), user);
        getPartition(pid).add(user.getId());
        if(maxIterationToNumUsersRatio != 1f) {
            maxIterations = (int) (Math.ceil(maxIterationToNumUsersRatio * getNumUsers()));
        }
        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }
    }

    @Override
    public void removeUser(Integer uid) {
        Set<Integer> friendIds = new HashSet<>(getUser(uid).getFriendIDs());
        for(Integer friendId : friendIds) {
            unfriend(uid, friendId);
        }
        getPartition(getPidForUser(uid)).remove(uid);
        uMap.remove(uid);
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        getUser(smallerUserId).befriend(largerUserId);
        getUser(largerUserId).befriend(smallerUserId);
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        getUser(smallerUserId).unfriend(largerUserId);
        getUser(largerUserId).unfriend(smallerUserId);
    }

    @Override
    public Integer addPartition() {
        int pid = nextPid;
        addPartition(pid);
        return pid;
    }

    @Override
    public void addPartition(Integer id) {
        pMap.put(id, new HashSet<Integer>());
        if(id >= nextPid) {
            nextPid = id + 1;
        }
    }

    @Override
    public void removePartition(Integer pid) {
        pMap.remove(pid);
    }

    @Override
    public Set<Integer> getPids() {
        return pMap.keySet();
    }

    @Override
    public Set<Integer> getPartition(Integer pid) {
        return pMap.get(pid);
    }

    @Override
    public Integer getPidForUser(Integer uid) {
        return uMap.get(uid).getBasePid();
    }

    public void repartition() {
        Results results = HRepartitioner.repartition(k, maxIterations, gamma, getPartitionToUsers(), getFriendships());
        int numMoves = results.getLogicalMoves();
        if(numMoves > 0) {
            increaseTallyLogical(numMoves);
            physicallyMigrate(results.getUidsToPids());
        }
    }

    void physicallyMigrate(Map<Integer, Integer> uidsToPids) {
        for(int uid : uidsToPids.keySet()) {
            int pid = uidsToPids.get(uid);
            User user = getUser(uid);
            if(user.getBasePid() != pid) {
                moveUser(uid, pid, false);
            }
        }
    }

    @Override
    public Integer getInitialPartitionId() {
        Integer minId = null;
        int minUsers = Integer.MAX_VALUE;
        for(Integer pid : pMap.keySet()) {
            int numUsers = getPartition(pid).size();
            if(numUsers < minUsers) {
                minUsers = numUsers;
                minId = pid;
            }
        }
        return minId;
    }

    @Override
    public User getUser(Integer uid) {
        return uMap.get(uid);
    }

    @Override
    public Integer getNumUsers() {
        return uMap.size();
    }

    @Override
    public Integer getNumPartitions() {
        return pMap.size();
    }

    @Override
    public Integer getEdgeCut() {
        int count = 0;
        for(int uid : uMap.keySet()) {
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

    @Override
    public Map<Integer,Set<Integer>> getPartitionToUsers() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for(Integer pid : getPids()) {
            map.put(pid, new HashSet<>(getPartition(pid)));
        }
        return map;
    }

    @Override
    public void moveUser(Integer uid, Integer pid, boolean omitFromTally) {
        User user = getUser(uid);
        getPartition(user.getBasePid()).remove(uid);
        getPartition(pid).add(uid);
        user.setBasePid(pid);
        if(!omitFromTally) {
            increaseTally(1);
        }
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : uMap.keySet()) {
            friendships.put(uid, new TreeSet<>(getUser(uid).getFriendIDs()));
        }
        return friendships;
    }

    @Override
    public Set<Integer> getUids() {
        return uMap.keySet();
    }

    public float getGamma() {
        return gamma;
    }

    @Override
    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * logicalMigrationTally);
    }

    @Override
    public void increaseTally(int amount) {
        migrationTally += amount;
    }

    @Override
    public void increaseTallyLogical(int amount) {
        logicalMigrationTally += amount;
    }

    @Override
    public String toString() {
        return "|gamma:" + gamma + "|probabilistic:" + probabilistic + "|#U:" + getNumUsers() + "|#P:" + pMap.size();
    }

    @Override
    public void checkValidity() {
        for(Integer uid : uMap.keySet()) {
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
            if(!observedMasterPid.equals(uMap.get(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's PID and system's");
            }
        }
    }

}
