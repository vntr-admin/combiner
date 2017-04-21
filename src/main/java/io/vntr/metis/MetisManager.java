package io.vntr.metis;

import io.vntr.User;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;
import static io.vntr.utils.ProbabilityUtils.getRandomElement;

/**
 * Created by robertlindquist on 12/5/16.
 */
public class MetisManager {

    private Map<Integer, User> uMap;
    private Map<Integer, Set<Integer>> partitions;
    private MetisRepartitioner repartitioner;

    private long migrationTally;

    private int nextPid = 1;
    private int nextUid = 1;

    public MetisManager(String gpmetisLocation, String gpmetisTempdir) {
        uMap = new HashMap<>();
        partitions = new HashMap<>();
        this.repartitioner = new MetisRepartitioner(gpmetisLocation, gpmetisTempdir);
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

    public Set<User> getUsers(Collection<Integer> uids) {
        Set<User> users = new HashSet<>();
        for(Integer uid : uids) {
            users.add(uMap.get(uid));
        }
        return users;
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

        int uid = user.getId();
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
        Map<Integer, Integer> newPartitioning = repartitioner.partition(getFriendships(), partitions.keySet());
        for(int uid : newPartitioning.keySet()) {
            int newPid = newPartitioning.get(uid);
            if(newPid != getUser(uid).getBasePid()) {
                moveUser(uid, newPid);
            }
        }
    }

    Integer getInitialPartitionId() {
        return getRandomElement(partitions.keySet());
    }

    public Integer addPartition() {
        int pid = nextPid;
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

    public void moveUser(Integer uid, Integer newPid) {
        User user = getUser(uid);
        if(partitions.containsKey(user.getBasePid())) {
            getPartition(user.getBasePid()).remove(uid);
        }
        getPartition(newPid).add(uid);
        user.setBasePid(newPid);
        increaseMigrationTally(1);
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

    public long getMigrationTally() {
        return migrationTally;
    }

    void increaseMigrationTally(int amount) {
        migrationTally += amount;
    }

    @Override
    public String toString() {
        return "#U:" + getNumUsers() + "|#P:" + getNumPartitions();
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