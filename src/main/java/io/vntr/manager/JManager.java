package io.vntr.manager;

import io.vntr.User;
import io.vntr.repartition.JRepartitioner;
import io.vntr.repartition.Results;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getRandomElement;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class JManager implements INoRepManager {

    private int k;
    private float alpha;
    private float initialT;
    private float deltaT;
    private int numRestarts;
    private boolean incremental;
    private long migrationTally;
    private long logicalMigrationTally;
    private double logicalMigrationRatio;

    private Map<Integer, User> uMap;
    private Map<Integer, Set<Integer>> partitions;

    private int nextPid = 1;
    private int nextUid = 1;

    public JManager(float alpha, float initialT, float deltaT, int k, int numRestarts, boolean incremental, double logicalMigrationRatio) {
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        this.numRestarts = numRestarts;
        this.incremental = incremental;
        this.logicalMigrationRatio = logicalMigrationRatio;
        uMap = new HashMap<>();
        partitions = new HashMap<>();
    }

    @Override
    public Set<Integer> getUids() {
        return uMap.keySet();
    }

    @Override
    public User getUser(Integer uid) {
        return uMap.get(uid);
    }

    @Override
    public Set<Integer> getPartition(Integer pid) {
        return partitions.get(pid);
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
        uMap.put(user.getId(), user);
        partitions.get(user.getBasePid()).add(user.getId());

        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }
    }

    @Override
    public void removeUser(Integer uid) {
        User user = uMap.remove(uid);
        for(Integer friendId : user.getFriendIDs()) {
            getUser(friendId).unfriend(uid);
        }
        partitions.get(user.getBasePid()).remove(uid);
    }

    @Override
    public void befriend(Integer id1, Integer id2) {
        getUser(id1).befriend(id2);
        getUser(id2).befriend(id1);
    }

    @Override
    public void unfriend(Integer id1, Integer id2) {
        getUser(id1).unfriend(id2);
        getUser(id2).unfriend(id1);
    }

    public void repartition() {
        Results results = JRepartitioner.repartition(alpha, initialT, deltaT, k, numRestarts, partitions, getFriendships(), incremental);
        increaseTallyLogical(results.getLogicalMoves());
        if(results.getUidsToPids() != null) {
            physicallyMigrate(results.getUidsToPids());
        }
    }

    void physicallyMigrate(Map<Integer, Integer> logicalPids) {
        for(Integer uid : logicalPids.keySet()) {
            User user = getUser(uid);
            Integer newPid = logicalPids.get(uid);
            if(!user.getBasePid().equals(newPid)) {
                moveUser(uid, newPid, false);
            }
        }
    }

    @Override
    public Integer getInitialPartitionId() {
        return getRandomElement(partitions.keySet());
    }

    @Override
    public Integer addPartition() {
        Integer pid = nextPid;
        addPartition(pid);
        return pid;
    }

    @Override
    public void addPartition(Integer pid) {
        partitions.put(pid, new HashSet<Integer>());
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    @Override
    public void removePartition(Integer partitionId) {
        partitions.remove(partitionId);
    }

    @Override
    public void moveUser(Integer uid, Integer newPid, boolean isFromPartitionRemoval) {
        User user = getUser(uid);
        if(partitions.containsKey(user.getBasePid())) {
            getPartition(user.getBasePid()).remove(uid);
        }
        getPartition(newPid).add(uid);
        user.setBasePid(newPid);

        if(!isFromPartitionRemoval) {
            increaseTally(1);
        }
    }

    @Override
    public Integer getNumPartitions() {
        return partitions.size();
    }

    @Override
    public Integer getNumUsers() {
        return uMap.size();
    }

    @Override
    public Integer getEdgeCut() {
        int count = 0;
        for(User user : uMap.values()) {
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
    public Map<Integer, Set<Integer>> getPartitionToUsers() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            map.put(pid, Collections.unmodifiableSet(partitions.get(pid)));
        }
        return map;
    }

    @Override
    public Set<Integer> getPids() {
        return partitions.keySet();
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : uMap.keySet()) {
            friendships.put(uid, getUser(uid).getFriendIDs());
        }
        return friendships;
    }

    @Override
    public String toString() {
        return "k:" + k + "|alpha:" + alpha + "|initialT:" + initialT + "|deltaT:" + deltaT + "|#U:" + getNumUsers() + "|#P:" + getNumPartitions();
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
    public void checkValidity() {
        for(Integer uid : uMap.keySet()) {
            Integer observedMasterPid = null;
            for(Integer pid : partitions.keySet()) {
                if(partitions.get(pid).contains(uid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot be in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must be in some partition");
            }
            if(!observedMasterPid.equals(uMap.get(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's PID and system's");
            }
        }
    }

    @Override
    public Integer getPidForUser(Integer uid) {
        for(Integer pid : partitions.keySet()) {
            if(partitions.get(pid).contains(uid)) {
                return pid;
            }
        }
        return null;
    }
}
