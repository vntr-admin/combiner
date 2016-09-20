package io.vntr.jabeja;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/20/16.
 */
public class JabejaManager {
    private Map<Long, JabejaUser> uMap;

    private int k;
    private double alpha;
    double initialTemp;
    double tempDelta;
    private int numPartitions;

    public JabejaManager(int k, double alpha) {
        uMap = new HashMap<Long, JabejaUser>();
        this.k = k;
        this.alpha = alpha;
    }

    public Long getPartitionForUser(Long uid) {
        return uMap.get(uid).getPid();
    }

    public JabejaUser getUser(Long uid) {
        return uMap.get(uid);
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
        //TODO: make sure there's not more to this
        Long tempPid = getUser(id1).getPid();
        getUser(id1).setPid(getUser(id2).getPid());
        getUser(id2).setPid(tempPid);
    }

    public void addUser(User user) {
        Long initialPartitionId = getInitialPartitionId();
        JabejaUser jabejaUser = new JabejaUser(user.getName(), user.getId(), initialPartitionId, k, alpha, this);
        uMap.put(jabejaUser.getId(), jabejaUser);
        //TODO: what else?
    }

    public void removeUser(Long uid) {
        JabejaUser user = uMap.remove(uid);
        for(Long friendId : user.getFriendIDs()) {
            getUser(friendId).unfriend(uid);
        }
        //TODO: what else?
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
        for(JabejaUser user : uMap.values()) {
            user.sampleAndSwap(initialTemp, tempDelta);
        }
    }

    Long getInitialPartitionId() {
        return 0L; //TODO: do this
    }
}
