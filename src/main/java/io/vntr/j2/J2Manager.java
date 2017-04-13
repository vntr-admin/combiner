package io.vntr.j2;

import io.vntr.User;

import java.util.*;

import static io.vntr.utils.ProbabilityUtils.getKDistinctValuesFromList;
import static io.vntr.utils.ProbabilityUtils.getRandomElement;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class J2Manager {

    private int k;
    private float alpha;
    private float initialT;
    private float deltaT;
    private long migrationTally;
    private long logicalMigrationTally;
    private double logicalMigrationRatio;

    private Map<Integer, J2User> uMap;
    private Map<Integer, Set<Integer>> partitions;

    private int nextPid = 1;
    private int nextUid = 1;

    public J2Manager(float alpha, float initialT, float deltaT, int k, double logicalMigrationRatio) {
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.k = k;
        this.logicalMigrationRatio = logicalMigrationRatio;
        uMap = new HashMap<>();
        partitions = new HashMap<>();
    }

    public Set<Integer> getUserIds() {
        return uMap.keySet();
    }

    public J2User getUser(Integer uid) {
        return uMap.get(uid);
    }

    public Set<Integer> getPartition(Integer pid) {
        return partitions.get(pid);
    }

    public Set<J2User> getUsers(Collection<Integer> uids) {
        Set<J2User> users = new HashSet<>();
        for(Integer uid : uids) {
            users.add(uMap.get(uid));
        }
        return users;
    }

    public Collection<J2User> getRandomSamplingOfUsers(int n) {
        if(uMap.size() <= n) {
            return getUsers(uMap.keySet());
        }
        else {
            return getUsers(getKDistinctValuesFromList(n, uMap.keySet()));
        }
    }

    public Collection<J2User> getRandomSamplingOfUsersOnPartition(int n, Integer pid) {
        if(partitions.get(pid).size() <= n) {
            return getUsers(partitions.get(pid));
        }
        else {
            return getUsers(getKDistinctValuesFromList(n, partitions.get(pid)));
        }
    }

    public void logicalSwap(Integer id1, Integer id2) {
        J2User u1 = getUser(id1);
        J2User u2 = getUser(id2);

        Integer logicalPid1 = u1.getLogicalPid();
        Integer logicalPid2 = u2.getLogicalPid();

        u1.setLogicalPid(logicalPid2);
        u2.setLogicalPid(logicalPid1);

        increaseLogicalMigrationTally(2);
    }

    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer initialPartitionId = getInitialPartitionId();
        int uid = user.getId();
        J2User j2User = new J2User(uid, initialPartitionId, alpha, this);
        addUser(j2User);
    }

    void addUser(J2User j2User) {
        uMap.put(j2User.getId(), j2User);
        partitions.get(j2User.getPid()).add(j2User.getId());

        if(j2User.getId() >= nextUid) {
            nextUid = j2User.getId() + 1;
        }
    }

    public void removeUser(Integer uid) {
        J2User user = uMap.remove(uid);
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

    void randomlyAssignLogicalPids() {
        List<Integer> pids = Arrays.asList(getPidsToAssign());
        Collections.shuffle(pids);

        int index = 0;
        for(J2User user : uMap.values()) {
            user.setLogicalPid(pids.get(index));
            index++;
        }
    }

    Integer[] getPidsToAssign() {
        //Fill array with pids such that:
        //(1) array.length = numUsers
        //(2) no pid occurs more than ceiling(numUsers/numPartitions) times
        //Note: The order is unimportant, since we'll shuffle it later
        Integer[] replicatedPids = new Integer[getNumUsers()];

        //Step 1: fill the first numPartitions * floor(numUsers/numPartitions) elements
        //This is easy, because we can just put floor(numUsers/numPartitions) copies of each pid
        int floorUsersPerPartition = getNumUsers() / getNumPartitions();

        int index = 0;
        for(int i : partitions.keySet()) {
            Arrays.fill(replicatedPids, index, index + floorUsersPerPartition, i);
            index += floorUsersPerPartition;
        }

        //Step 2: fill the remainder (if any) with randomly-selected pids (no more than once each)
        List<Integer> remainingPids = new ArrayList<>(partitions.keySet());
        Collections.shuffle(remainingPids);
        Iterator<Integer> pidIter = remainingPids.iterator();
        while(index < replicatedPids.length) {
            replicatedPids[index] = pidIter.next();
            index++;
        }

        return replicatedPids;
    }

    void physicallyMigrate() {
        for(J2User user : uMap.values()) {
            Integer bestLogicalPid = user.getBestLogicalPid();
            if(bestLogicalPid != null && !bestLogicalPid.equals(user.getPid())) {
                moveUser(user.getId(), bestLogicalPid, false);
            }
        }
    }

    public void repartition() {
        boolean changed = false;
        int bestEdgeCut = getEdgeCut(false);

        int numRestarts = 10;
        for(int i=0; i<numRestarts; i++) {
            randomlyAssignLogicalPids();
            for(float t = initialT; t >= 1; t -= deltaT) {
                List<Integer> randomUserList = new LinkedList<>(uMap.keySet());
                Collections.shuffle(randomUserList);
                for(Integer uid : randomUserList) {
                    J2User user = getUser(uid);
                    J2User partner = user.findPartner(getRandomSamplingOfUsersOnPartition(k, user.getPid()), t);
                    if(partner == null) {
                        partner = user.findPartner(getRandomSamplingOfUsers(k), t);
                    }
                    if(partner != null) {
                        logicalSwap(user.getId(), partner.getId());
                    }
                }
            }
            int edgeCut = getEdgeCut(true);
            if(edgeCut < bestEdgeCut) {
                changed = true;
                bestEdgeCut = edgeCut;
                for(J2User user : uMap.values()) {
                    user.setBestLogicalPid(user.getLogicalPid());
                }
            }
        }

        if(changed) {
            physicallyMigrate();
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

    public void moveUser(Integer uid, Integer newPid, boolean isFromPartitionRemoval) {
        J2User user = getUser(uid);
        if(partitions.containsKey(user.getPid())) {
            getPartition(user.getPid()).remove(uid);
        }
        getPartition(newPid).add(uid);
        user.setPid(newPid);

        if(!isFromPartitionRemoval) {
            increaseMigrationTally(1);
        }
    }

    public Integer getNumPartitions() {
        return partitions.size();
    }

    public Integer getNumUsers() {
        return uMap.size();
    }

    public Integer getEdgeCut(boolean logical) {
        int count = 0;
        for(J2User user : uMap.values()) {
            Integer userPid = logical ? user.getLogicalPid() : user.getPid();

            for(int friendId : user.getFriendIDs()) {
                Integer friendPid = logical ? getUser(friendId).getLogicalPid() : getUser(friendId).getPid();
                if(userPid < friendPid) {
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

    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * logicalMigrationTally);
    }

    void increaseMigrationTally(int amount) {
        migrationTally += amount;
    }

    void increaseLogicalMigrationTally(int amount) {
        logicalMigrationTally += amount;
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
