package io.vntr.manager;

import io.vntr.User;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getRandomElement;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class NoRepManager {

    private Map<Integer, User> uMap;
    private Map<Integer, Set<Integer>> pMap;

    private boolean placeNewUserRandomly;

    private long migrationTally;
    private long logicalMigrationTally;
    private double logicalMigrationRatio;

    private int nextPid = 1;
    private int nextUid = 1;

    public NoRepManager(double logicalMigrationRatio, boolean placeNewUserRandomly) {
        this.placeNewUserRandomly = placeNewUserRandomly;
        this.logicalMigrationRatio = logicalMigrationRatio;
        uMap = new HashMap<>();
        pMap = new HashMap<>();
    }

    public Set<Integer> getUids() {
        return uMap.keySet();
    }

    public User getUser(Integer uid) {
        return uMap.get(uid);
    }

    public Set<Integer> getPartition(Integer pid) {
        return pMap.get(pid);
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
        pMap.get(user.getBasePid()).add(user.getId());
        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }
    }

    public void removeUser(Integer uid) {
        Set<Integer> friendIds = new HashSet<>(getUser(uid).getFriendIDs());
        for(Integer friendId : friendIds) {
            unfriend(uid, friendId);
        }
        getPartition(getPidForUser(uid)).remove(uid);
        uMap.remove(uid);
    }

    public void befriend(Integer id1, Integer id2) {
        getUser(id1).befriend(id2);
        getUser(id2).befriend(id1);
    }

    public void unfriend(Integer id1, Integer id2) {
        getUser(id1).unfriend(id2);
        getUser(id2).unfriend(id1);
    }

    public Integer getInitialPartitionId() {
        if(placeNewUserRandomly) {
            return getRandomElement(pMap.keySet());
        }
        else {
            int minUsers = Integer.MAX_VALUE;
            Integer minPartition = null;
            for(int pid : pMap.keySet()) {
                if(pMap.get(pid).size() < minUsers) {
                    minUsers = pMap.get(pid).size();
                    minPartition = pid;
                }
            }
            return minPartition;
        }
    }

    public Integer addPartition() {
        int pid = nextPid;
        addPartition(pid);
        return pid;
    }

    public void addPartition(Integer pid) {
        pMap.put(pid, new HashSet<Integer>());
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    public void removePartition(Integer pid) {
        pMap.remove(pid);
    }

    public Integer getNumUsers() {
        return uMap.size();
    }

    public Integer getNumPartitions() {
        return pMap.size();
    }

    public Integer getEdgeCut() {
        int count = 0;
        for(User user : uMap.values()) {
            for(int friendId : user.getFriendIDs()) {
                if(user.getBasePid() < getUser(friendId).getBasePid()) {
                    count++;
                }
            }
        }
        return count;
    }

    public Map<Integer, Set<Integer>> getPartitionToUsers() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for(Integer pid : getPids()) {
            map.put(pid, new HashSet<>(pMap.get(pid)));
        }
        return map;
    }

    public void moveUser(Integer uid, Integer pid, boolean omitFromTally) {
        User user = getUser(uid);
        pMap.get(user.getBasePid()).remove(uid);
        getPartition(pid).add(uid);
        user.setBasePid(pid);
        if(!omitFromTally) {
            increaseTally(1);
        }
    }

    public Set<Integer> getPids() {
        return pMap.keySet();
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : uMap.keySet()) {
            friendships.put(uid, new HashSet<>(getUser(uid).getFriendIDs()));
        }
        return friendships;
    }

    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * logicalMigrationTally);
    }

    public void increaseTally(int amount) {
        migrationTally += amount;
    }

    public void increaseTallyLogical(int amount) {
        logicalMigrationTally += amount;
    }

    @Override
    public String toString() {
        return "#U:" + getNumUsers() + "|#P:" + getNumPartitions();
    }

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

    public Integer getPidForUser(Integer uid) {
        return uMap.get(uid).getBasePid();
    }

}
