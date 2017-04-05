package io.vntr.jabeja;

import io.vntr.User;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.*;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class JabejaManager {

    private int k;
    private float alpha;
    private float initialT;
    private float deltaT;
    private float befriendInitialT;
    private float befriendDeltaT;
    private long migrationTally;

    private NavigableMap<Integer, JabejaUser> uMap;
    private NavigableMap<Integer, Set<Integer>> partitions;

    private static final Integer defaultInitialPid = 1;

    public JabejaManager(float alpha, float initialT, float deltaT, float befriendInitialT, float befriendDeltaT, int k) {
        this.alpha = alpha;
        this.initialT = initialT;
        this.befriendDeltaT = befriendDeltaT;
        this.befriendInitialT = befriendInitialT;
        this.deltaT = deltaT;
        this.k = k;
        uMap = new TreeMap<>();
        partitions = new TreeMap<>();
    }

    public Integer getPartitionForUser(Integer uid) {
        return uMap.get(uid).getPid();
    }

    public Set<Integer> getUserIds() {
        return uMap.keySet();
    }

    public JabejaUser getUser(Integer uid) {
        return uMap.get(uid);
    }

    public Set<Integer> getPartition(Integer pid) {
        return partitions.get(pid);
    }

    public Set<JabejaUser> getUsers(Collection<Integer> uids) {
        Set<JabejaUser> users = new HashSet<>();
        for(Integer uid : uids) {
            users.add(uMap.get(uid));
        }
        return users;
    }

    public Collection<JabejaUser> getRandomSamplingOfUsers(int n) {
        return getUsers(getKDistinctValuesFromList(n, uMap.keySet()));
    }

    public void swap(Integer id1, Integer id2) {
        JabejaUser u1 = getUser(id1);
        JabejaUser u2 = getUser(id2);

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
        int newUid = uMap.lastKey() + 1;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer initialPartitionId = getInitialPartitionId();
        JabejaUser jabejaUser = new JabejaUser(user.getId(), initialPartitionId, alpha, this);
        addUser(jabejaUser);
    }

    void addUser(JabejaUser jabejaUser) {
        uMap.put(jabejaUser.getId(), jabejaUser);
        partitions.get(jabejaUser.getPid()).add(jabejaUser.getId());
    }

    public void removeUser(Integer uid) {
        JabejaUser user = uMap.remove(uid);
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

    public void repartition(boolean isDowntime) {
        float effectiveDeltaT = isDowntime ? deltaT : befriendDeltaT;
        float effectiveInitialT = isDowntime ? initialT : befriendInitialT;
        for(float t = effectiveInitialT; t >= 1; t -= effectiveDeltaT) {
            List<Integer> randomUserList = new LinkedList<>(uMap.keySet());
            Collections.shuffle(randomUserList);
            for(Integer uid : randomUserList) {
                JabejaUser user = getUser(uid);
                JabejaUser partner = user.findPartner(getUsers(user.getFriendIDs()), t);
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
        JabejaUser user = getUser(uid);
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
        for(JabejaUser user : uMap.values()) {
            for(int friendId : user.getFriendIDs()) {
                JabejaUser friend = getUser(friendId);
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
}
