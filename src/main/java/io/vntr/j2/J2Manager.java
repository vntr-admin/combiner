package io.vntr.j2;

import io.vntr.User;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;
import static io.vntr.utils.ProbabilityUtils.getRandomElement;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class J2Manager {

    private int k;
    private float alpha;
    private float initialT;
    private float deltaT;
    private int numRestarts;
    private long migrationTally;
    private long logicalMigrationTally;
    private double logicalMigrationRatio;

    private Map<Integer, User> uMap;
    private Map<Integer, Set<Integer>> partitions;

    private J2Repartitioner repartitioner;

    private int nextPid = 1;
    private int nextUid = 1;

    public J2Manager(float alpha, float initialT, float deltaT, int k, int numRestarts, double logicalMigrationRatio) {
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        this.numRestarts = numRestarts;
        this.logicalMigrationRatio = logicalMigrationRatio;
        uMap = new HashMap<>();
        this.repartitioner = new J2Repartitioner(this, alpha, initialT, deltaT, k);
        partitions = new HashMap<>();
    }

    public Set<Integer> getUserIds() {
        return uMap.keySet();
    }

    public User getUser(Integer uid) {
        return uMap.get(uid);
    }

    public Set<Integer> getPartition(Integer pid) {
        return partitions.get(pid);
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
        uMap.put(user.getId(), user);
        partitions.get(user.getBasePid()).add(user.getId());

        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }
    }

    public void removeUser(Integer uid) {
        User user = uMap.remove(uid);
        for(Integer friendId : user.getFriendIDs()) {
            getUser(friendId).unfriend(uid);
        }
        partitions.get(user.getBasePid()).remove(uid);
    }

    public void befriend(Integer id1, Integer id2) {
        getUser(id1).befriend(id2);
        getUser(id2).befriend(id1);
    }

    public void unfriend(Integer id1, Integer id2) {
        getUser(id1).unfriend(id2);
        getUser(id2).unfriend(id1);
    }

    public void repartition() {
        increaseLogicalMigrationTally(repartitioner.repartition(numRestarts));
    }

    Integer getInitialPartitionId() {
        return getRandomElement(partitions.keySet());
    }

    public Integer addPartition() {
        Integer pid = nextPid;
        addPartition(pid);
        return pid;
    }

    void addPartition(Integer pid) {
        partitions.put(pid, new HashSet<Integer>());
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    public void removePartition(Integer partitionId) {
        partitions.remove(partitionId);
    }

    public Collection<Integer> getPartitionIds() {
        return partitions.keySet();
    }

    public void moveUser(Integer uid, Integer newPid, boolean isFromPartitionRemoval) {
        User user = getUser(uid);
        if(partitions.containsKey(user.getBasePid())) {
            getPartition(user.getBasePid()).remove(uid);
        }
        getPartition(newPid).add(uid);
        user.setBasePid(newPid);

        if(!isFromPartitionRemoval) {
            increaseMigrationTally(1);
        }
    }

    public Integer getNumPartitions() {
        return partitions.size();
    }

    public Integer getNumUsers() {
        return uMap.size();
    }

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

    public Map<Integer, Set<Integer>> getPartitionToUsers() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for(Integer pid : partitions.keySet()) {
            map.put(pid, Collections.unmodifiableSet(partitions.get(pid)));
        }
        return map;
    }

    public Set<Integer> getAllPartitionIds() {
        return partitions.keySet();
    }

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

    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * logicalMigrationTally);
    }

    void increaseMigrationTally(int amount) {
        migrationTally += amount;
    }

    void increaseLogicalMigrationTally(int amount) {
        logicalMigrationTally += amount;
    }

    void checkValidity() {
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
}
