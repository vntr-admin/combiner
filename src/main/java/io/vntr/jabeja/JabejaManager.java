package io.vntr.jabeja;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class JabejaManager {

    private int k;
    private double alpha;
    private double initialT;
    private double deltaT;

    private Map<Long, JabejaUser> uMap;
    private NavigableMap<Long, Set<Long>> partitions;

    private static final Long defaultInitialPid = 1L;

    public JabejaManager(double alpha, double initialT, double deltaT, int k) {
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        uMap = new HashMap<Long, JabejaUser>();
        partitions = new TreeMap<Long, Set<Long>>();
    }

    public Long getPartitionForUser(Long uid) {
        return uMap.get(uid).getPid();
    }

    public JabejaUser getUser(Long uid) {
        return uMap.get(uid);
    }

    public Set<Long> getPartition(Long pid) {
        return partitions.get(pid);
    }

    public Collection<JabejaUser> getRandomSamplingOfUsers(int n) {
        Set<JabejaUser> users = new HashSet<JabejaUser>();
        Set<Long> ids = ProbabilityUtils.getKDistinctValuesFromList(n, new LinkedList<Long>(uMap.keySet()));
        for(Long id : ids) {
            users.add(uMap.get(id));
        }
        return users;
    }

    public void swap(Long id1, Long id2) {
        JabejaUser u1 = getUser(id1);
        JabejaUser u2 = getUser(id2);

        Long tempPid = u1.getPid();
        u1.setPid(u2.getPid());
        u2.setPid(tempPid);

        getPartition(u1.getPid()).add(u1.getId());
        getPartition(u2.getPid()).add(u2.getId());
        getPartition(u1.getPid()).remove(u2.getId());
        getPartition(u2.getPid()).remove(u1.getId());
    }

    public void addUser(User user) {
        Long initialPartitionId = getInitialPartitionId();
        JabejaUser jabejaUser = new JabejaUser(user.getName(), user.getId(), initialPartitionId, alpha, this);
        addUser(jabejaUser);
    }

    void addUser(JabejaUser jabejaUser) {
        uMap.put(jabejaUser.getId(), jabejaUser);
        partitions.get(jabejaUser.getPid()).add(jabejaUser.getId());
    }

    public void removeUser(Long uid) {
        JabejaUser user = uMap.remove(uid);
        for(Long friendId : user.getFriendIDs()) {
            getUser(friendId).unfriend(uid);
        }
        partitions.get(user.getPid()).remove(uid);
    }

    public void befriend(Long id1, Long id2) {
        getUser(id1).befriend(id2);
        getUser(id2).befriend(id1);
    }

    public void unfriend(Long id1, Long id2) {
        getUser(id1).unfriend(id2);
        getUser(id2).unfriend(id1);
    }

    public void repartition() {
        for(double t = initialT; t >= 1; t -= deltaT) {
            List<Long> randomUserList = new LinkedList<Long>(uMap.keySet());
            Collections.shuffle(randomUserList);
            for(Long uid : randomUserList) {
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

    Long getInitialPartitionId() {
        return ProbabilityUtils.getKDistinctValuesFromList(1, new LinkedList<Long>(partitions.keySet())).iterator().next();
    }

    public Long addPartition() {
        Long pid = partitions.isEmpty() ? defaultInitialPid : partitions.lastKey() + 1L;
        partitions.put(pid, new HashSet<Long>());
        return pid;
    }

    public void removePartition(Long partitionId) {
        partitions.remove(partitionId);
    }

    public Collection<Long> getPartitionIds() {
        return partitions.keySet();
    }

    public void moveUser(Long uid, Long newPid) {
        JabejaUser user = getUser(uid);
        if(partitions.containsKey(user.getPid())) {
            getPartition(user.getPid()).remove(uid);
        }
        getPartition(newPid).add(uid);
        user.setPid(newPid);

    }
}
