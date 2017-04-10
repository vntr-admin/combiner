package io.vntr.metis;

import io.vntr.User;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;
import static io.vntr.utils.ProbabilityUtils.getRandomElement;

/**
 * Created by robertlindquist on 12/5/16.
 */
public class MetisManager {

    private Map<Integer, MetisUser> uMap;
    private Map<Integer, Set<Integer>> partitions;
    private MetisRepartitioner repartitioner;

    private long migrationTally;

    private static final Integer defaultInitialPid = 1;
    private int nextPid = 1;
    private int nextUid = 1;

    public MetisManager(String gpmetisLocation, String gpmetisTempdir) {
        uMap = new HashMap<>();
        partitions = new HashMap<>();
        this.repartitioner = new MetisRepartitioner(gpmetisLocation, gpmetisTempdir);
    }

    public Integer getPartitionForUser(Integer uid) {
        return uMap.get(uid).getPid();
    }

    public Set<Integer> getUserIds() {
        return uMap.keySet();
    }

    public MetisUser getUser(Integer uid) {
        return uMap.get(uid);
    }

    public Set<Integer> getPartition(Integer pid) {
        return partitions.get(pid);
    }

    public Set<MetisUser> getUsers(Collection<Integer> uids) {
        Set<MetisUser> users = new HashSet<>();
        for(Integer uid : uids) {
            users.add(uMap.get(uid));
        }
        return users;
    }

    public Collection<MetisUser> getRandomSamplingOfUsers(int n) {
        return getUsers(getKDistinctValuesFromList(n, uMap.keySet()));
    }

    public void swap(Integer id1, Integer id2) {
        MetisUser u1 = getUser(id1);
        MetisUser u2 = getUser(id2);

        int pid1 = u1.getPid();
        int pid2 = u2.getPid();

        u1.setPid(pid2);
        u2.setPid(pid1);

        getPartition(pid2).add(id1);
        getPartition(pid1).add(id2);
        getPartition(pid2).remove(id2);
        getPartition(pid1).remove(id1);
    }

    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer initialPartitionId = getInitialPartitionId();
        int uid = user.getId();
        MetisUser MetisUser = new MetisUser(uid, initialPartitionId);
        addUser(MetisUser);
        if(uid >= nextUid) {
            nextUid = uid + 1;
        }
    }

    void addUser(MetisUser MetisUser) {
        uMap.put(MetisUser.getId(), MetisUser);
        partitions.get(MetisUser.getPid()).add(MetisUser.getId());
    }

    public void removeUser(Integer uid) {
        MetisUser user = uMap.remove(uid);
        for(Integer friendId : user.getFriendIDs()) {
            getUser(friendId).unfriend(uid);
        }
        partitions.get(user.getPid()).remove(uid);
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
            if(newPid != getUser(uid).getPid()) {
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
        MetisUser user = getUser(uid);
        if(partitions.containsKey(user.getPid())) {
            getPartition(user.getPid()).remove(uid);
        }
        getPartition(newPid).add(uid);
        user.setPid(newPid);
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
        for(MetisUser user : uMap.values()) {
            for(int friendId : user.getFriendIDs()) {
                MetisUser friend = getUser(friendId);
                if(user.getPid() < friend.getPid()) {
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
            if(!observedMasterPid.equals(uMap.get(uid).getPid())) {
                throw new RuntimeException("Mismatch between user's PID and system's");
            }
        }
    }
}