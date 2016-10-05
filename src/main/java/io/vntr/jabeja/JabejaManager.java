package io.vntr.jabeja;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class JabejaManager {

    private int k;
    private float alpha;
    private float initialT;
    private float deltaT;

    private Map<Integer, JabejaUser> uMap;
    private NavigableMap<Integer, Set<Integer>> partitions;

    private static final Integer defaultInitialPid = 1;

    public JabejaManager(float alpha, float initialT, float deltaT, int k) {
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        uMap = new HashMap<Integer, JabejaUser>();
        partitions = new TreeMap<Integer, Set<Integer>>();
    }

    public Integer getPartitionForUser(Integer uid) {
        return uMap.get(uid).getPid();
    }

    public JabejaUser getUser(Integer uid) {
        return uMap.get(uid);
    }

    public Set<Integer> getPartition(Integer pid) {
        return partitions.get(pid);
    }

    public Collection<JabejaUser> getRandomSamplingOfUsers(int n) {
        Set<JabejaUser> users = new HashSet<JabejaUser>();
        Set<Integer> ids = ProbabilityUtils.getKDistinctValuesFromList(n, uMap.keySet());
        for(Integer id : ids) {
            users.add(uMap.get(id));
        }
        return users;
    }

    public void swap(Integer id1, Integer id2) {
        JabejaUser u1 = getUser(id1);
        JabejaUser u2 = getUser(id2);

        Integer tempPid = u1.getPid();
        u1.setPid(u2.getPid());
        u2.setPid(tempPid);

        getPartition(u1.getPid()).add(u1.getId());
        getPartition(u2.getPid()).add(u2.getId());
        getPartition(u1.getPid()).remove(u2.getId());
        getPartition(u2.getPid()).remove(u1.getId());
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

    public void repartition() {
        for(float t = initialT; t >= 1; t -= deltaT) {
            List<Integer> randomUserList = new LinkedList<Integer>(uMap.keySet());
            Collections.shuffle(randomUserList);
            for(Integer uid : randomUserList) {
                JabejaUser user = getUser(uid);
                JabejaUser partner = user.findPartner(user.getFriends(), t);
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
        return ProbabilityUtils.getKDistinctValuesFromList(1, partitions.keySet()).iterator().next();
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
        return (int) partitions.size();
    }

    public Integer getNumUsers() {
        return (int) uMap.size();
    }

    public Integer getEdgeCut() {
        int count = 0;
        for(JabejaUser user : uMap.values()) {
            for(JabejaUser friend : user.getFriends()) {
                if(user.getPid().intValue() < friend.getPid().intValue()) {
                    count++;
                }
            }
        }
        return count;
    }

    public Map<Integer, Set<Integer>> getPartitionToUsers() {
        Map<Integer, Set<Integer>> map = new HashMap<Integer, Set<Integer>>();
        for(Integer pid : partitions.keySet()) {
            map.put(pid, Collections.unmodifiableSet(partitions.get(pid)));
        }
        return map;
    }

    public Set<Integer> getAllPartitionIds() {
        return partitions.keySet();
    }
}
