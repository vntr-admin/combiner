package io.vntr.jabar;

import io.vntr.User;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;
import static io.vntr.utils.ProbabilityUtils.getRandomElement;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class JabarManager {

    private int k;
    private float alpha;
    private float initialT;
    private float deltaT;
    private long migrationTally;

    private Map<Integer, JabarUser> uMap;
    private Map<Integer, Set<Integer>> partitions;

    private int nextPid = 1;
    private int nextUid = 1;

    public JabarManager(float alpha, float initialT, float deltaT, int k) {
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        uMap = new HashMap<>();
        partitions = new HashMap<>();
    }

    public Integer getPartitionForUser(Integer uid) {
        return uMap.get(uid).getPid();
    }

    public Set<Integer> getUserIds() {
        return uMap.keySet();
    }

    public JabarUser getUser(Integer uid) {
        return uMap.get(uid);
    }

    public Set<Integer> getPartition(Integer pid) {
        return partitions.get(pid);
    }

    public Set<JabarUser> getUsers(Collection<Integer> uids) {
        Set<JabarUser> users = new HashSet<>();
        for(Integer uid : uids) {
            users.add(uMap.get(uid));
        }
        return users;
    }

    public Collection<JabarUser> getRandomSamplingOfUsers(int n) {
        return getUsers(getKDistinctValuesFromList(n, uMap.keySet()));
    }

    public void swap(Integer id1, Integer id2) {
        JabarUser u1 = getUser(id1);
        JabarUser u2 = getUser(id2);

        int pid1 = u1.getPid();
        int pid2 = u2.getPid();

        u1.setPid(pid2);
        u2.setPid(pid1);

        getPartition(pid2).add(id1);
        getPartition(pid1).add(id2);
        getPartition(pid2).remove(id2);
        getPartition(pid1).remove(id1);
        increaseMigrationTally(2);
    }

    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer initialPartitionId = getInitialPartitionId();
        int uid = user.getId();
        JabarUser jabarUser = new JabarUser(uid, initialPartitionId, alpha, this);
        addUser(jabarUser);
        if(uid >= nextUid) {
            nextUid = uid + 1;
        }
    }

    void addUser(JabarUser jabarUser) {
        uMap.put(jabarUser.getId(), jabarUser);
        partitions.get(jabarUser.getPid()).add(jabarUser.getId());
    }

    public void removeUser(Integer uid) {
        JabarUser user = uMap.remove(uid);
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
        for(float t = initialT; t >= 1; t -= deltaT) {
            List<Integer> randomUserList = new LinkedList<>(uMap.keySet());
            Collections.shuffle(randomUserList);
            for(Integer uid : randomUserList) {
                JabarUser user = getUser(uid);
                JabarUser partner = user.findPartner(getUsers(user.getFriendIDs()), t);
                if(partner == null) {
                    partner = user.findPartner(getRandomSamplingOfUsers(k), t);
                }
                if(partner != null) {
                    swap(user.getId(), partner.getId());
                }
            }
        }
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

    public void moveUser(Integer uid, Integer newPid) {
        JabarUser user = getUser(uid);
        if(partitions.containsKey(user.getPid())) {
            getPartition(user.getPid()).remove(uid);
        }
        getPartition(newPid).add(uid);
        user.setPid(newPid);
    }

    public Integer getNumPartitions() {
        return partitions.size();
    }

    public Integer getNumUsers() {
        return uMap.size();
    }

    public Integer getEdgeCut() {
        int count = 0;
        for(JabarUser user : uMap.values()) {
            for(int friendId : user.getFriendIDs()) {
                JabarUser friend = getUser(friendId);
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

    @Override
    public String toString() {
        return "k:" + k + "|alpha:" + alpha + "|initialT:" + initialT + "|deltaT:" + deltaT + "|#U:" + getNumUsers() + "|#P:" + getNumPartitions();
    }

    public Long getMigrationTally() {
        return migrationTally;
    }

    void incrementMigrationTally() {
        increaseMigrationTally(1);
    }

    void increaseMigrationTally(int amount) {
        migrationTally += amount;
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


    void rebalance(Integer smallerUserId, Integer largerUserId) {
        JabarUser smallerUser = getUser(smallerUserId);
        int smallerPid = smallerUser.getPid();

        JabarUser largerUser  = getUser(largerUserId);
        int largerPid = largerUser.getPid();

        if(smallerPid != largerPid) {
            JabarUser smallerPartner = findPartnerOnPartition(smallerUser, largerPid);
            JabarUser largerPartner = findPartnerOnPartition(largerUser, smallerPid);

            if(smallerPartner != null && largerPartner == null) {
                swap(smallerUserId, smallerPartner.getId());
            }
            else if(largerPartner != null && smallerPartner == null) {
                swap(largerUserId, largerPartner.getId());
            }
            else if(smallerPartner != null && largerPartner != null) {
                int gainSmallerToLarger = calculateGain(smallerPartner, largerPartner);
                int gainLargerToSmaller = calculateGain(largerPartner, smallerPartner);
                if(gainSmallerToLarger >= gainLargerToSmaller) {
                    swap(smallerUserId, smallerPartner.getId());
                }
                else {
                    swap(largerUserId, largerPartner.getId());
                }
            }
        }
    }

    JabarUser findPartnerOnPartition(JabarUser user, Integer pid) {
        Set<Integer> partition = partitions.get(pid);
        Set<Integer> candidates;
        if(partition.size() <= k) {
            candidates = new HashSet<>(partition);
        }
        else {
            candidates = getKDistinctValuesFromList(k, partition);
        }
        return user.findPartner(getUsers(candidates), 1F);
    }

    int calculateGain(JabarUser user1, JabarUser user2) {
        int oldCut = user1.getNeighborsOnPartition(user2.getPid()) + user2.getNeighborsOnPartition(user1.getPid());
        int newCut = user1.getNeighborsOnPartition(user1.getPid()) + user2.getNeighborsOnPartition(user2.getPid());
        return oldCut - newCut;
    }
}
