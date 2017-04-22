package io.vntr.metis;

import io.vntr.INoRepManager;
import io.vntr.User;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;
import static io.vntr.utils.ProbabilityUtils.getRandomElement;

/**
 * Created by robertlindquist on 12/5/16.
 */
public class MetisManager implements INoRepManager {

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

    public Set<User> getUsers(Collection<Integer> uids) {
        Set<User> users = new HashSet<>();
        for(Integer uid : uids) {
            users.add(uMap.get(uid));
        }
        return users;
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

        int uid = user.getId();
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

    @Override
    public Integer getPidForUser(Integer uid) {
        for(Integer pid : partitions.keySet()) {
            if(partitions.get(pid).contains(uid)) {
                return pid;
            }
        }
        return null;
    }

    public void repartition() {
        Map<Integer, Integer> newPartitioning = repartitioner.partition(getFriendships(), partitions.keySet());
        for(int uid : newPartitioning.keySet()) {
            int newPid = newPartitioning.get(uid);
            if(newPid != getUser(uid).getBasePid()) {
                moveUser(uid, newPid, true);
            }
        }
    }

    @Override
    public Integer getInitialPartitionId() {
        return getRandomElement(partitions.keySet());
    }

    @Override
    public Integer addPartition() {
        int pid = nextPid;
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
    public Set<Integer> getPids() {
        return partitions.keySet();
    }

    @Override
    public void moveUser(Integer uid, Integer newPid, boolean omitFromTally) {
        User user = getUser(uid);
        if(partitions.containsKey(user.getBasePid())) {
            getPartition(user.getBasePid()).remove(uid);
        }
        getPartition(newPid).add(uid);
        user.setBasePid(newPid);
        if(!omitFromTally) {
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
            for(int friendId : user.getFriendIDs()) {
                User friend = getUser(friendId);
                if(user.getBasePid() < friend.getBasePid()) {
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

    @Override
    public void increaseTally(int amount) {
        migrationTally += amount;
    }

    @Override
    public void increaseTallyLogical(int amount) {
        //There's no such thing for METIS
    }

    @Override
    public String toString() {
        return "#U:" + getNumUsers() + "|#P:" + getNumPartitions();
    }

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
}