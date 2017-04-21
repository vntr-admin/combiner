package io.vntr.dummy;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class DummyManager {
    private NavigableMap<Integer, Set<Integer>> partitions;
    private NavigableMap<Integer, User> uMap;

    private static final Integer defaultInitialPid = 1;

    public DummyManager() {
        this.partitions = new TreeMap<>();
        this.uMap = new TreeMap<>();
    }

    public Integer getPartitionForUser(Integer uid) {
        return uMap.get(uid).getBasePid();
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
        int newUid = uMap.lastKey() + 1;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        if(user.getBasePid() == null) {
            user.setBasePid(getInitialPartitionId());
        }
        uMap.put(user.getId(), user);
        partitions.get(user.getBasePid()).add(user.getId());
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

    Integer getInitialPartitionId() {
        int minUsers = Integer.MAX_VALUE;
        Integer minPartition = null;
        for(int pid : partitions.keySet()) {
            if(partitions.get(pid).size() < minUsers) {
                minUsers = partitions.get(pid).size();
                minPartition = pid;
            }
        }
        return minPartition;
    }

    public Integer addPartition() {
        Integer pid = partitions.isEmpty() ? defaultInitialPid : partitions.lastKey() + 1;
        addPartition(pid);
        return pid;
    }

    void addPartition(Integer pid) {
        partitions.put(pid, new HashSet<Integer>());
    }

    public void removePartition(Integer partitionId) {
        partitions.remove(partitionId);
    }

    public Collection<Integer> getPartitionIds() {
        return partitions.keySet();
    }

    public void moveUser(Integer uid, Integer newPid) {
        User user = getUser(uid);
        if(partitions.containsKey(user.getBasePid())) {
            getPartition(user.getBasePid()).remove(uid);
        }
        getPartition(newPid).add(uid);
        user.setBasePid(newPid);
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
            for(int friendId : user.getFriendIDs()) {
                User friend = getUser(friendId);
                if(user.getBasePid() < friend.getBasePid()) {
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

    @Override
    public String toString() {
        return "{Dummy} #U:" + getNumUsers() + "|#P:" + getNumPartitions();
    }

}
