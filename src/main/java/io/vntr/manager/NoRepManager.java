package io.vntr.manager;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.User;

import static io.vntr.utils.TroveUtils.getRandomElement;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class NoRepManager {

    private TIntObjectMap<User> uMap;
    private TIntObjectMap<TIntSet> pMap;

    private final boolean placeNewUserRandomly;

    private long migrationTally;
    private long logicalMigrationTally;
    private final double logicalMigrationRatio;

    private int nextPid = 1;
    private int nextUid = 1;

    public NoRepManager(double logicalMigrationRatio, boolean placeNewUserRandomly) {
        this.placeNewUserRandomly = placeNewUserRandomly;
        this.logicalMigrationRatio = logicalMigrationRatio;
        uMap = new TIntObjectHashMap<>();
        pMap = new TIntObjectHashMap<>();
    }

    public TIntSet getUids() {
        return uMap.keySet();
    }

    public User getUser(Integer uid) {
        return uMap.get(uid);
    }

    public TIntSet getPartition(Integer pid) {
        return pMap.get(pid);
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
        uMap.put(user.getId(), user);
        pMap.get(user.getBasePid()).add(user.getId());
        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }
    }

    public void removeUser(Integer uid) {
        TIntSet friendIds = new TIntHashSet(getUser(uid).getFriendIDs());
        for(TIntIterator iter = friendIds.iterator(); iter.hasNext(); ) {
            unfriend(uid, iter.next());
        }
        getPartition(getPidForUser(uid)).remove(uid);
        uMap.remove(uid);
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
        if(placeNewUserRandomly) {
            return getRandomElement(pMap.keySet());
        }
        else {
            int minUsers = Integer.MAX_VALUE;
            Integer minPartition = null;
            for(int pid : pMap.keys()) {
                if(pMap.get(pid).size() < minUsers) {
                    minUsers = pMap.get(pid).size();
                    minPartition = pid;
                }
            }
            return minPartition;
        }
    }

    public Integer addPartition() {
        int pid = nextPid;
        addPartition(pid);
        return pid;
    }

    public void addPartition(Integer pid) {
        pMap.put(pid, new TIntHashSet());
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    public void removePartition(Integer pid) {
        pMap.remove(pid);
    }

    public Integer getNumUsers() {
        return uMap.size();
    }

    public Integer getNumPartitions() {
        return pMap.size();
    }

    public Integer getEdgeCut() {
        int count = 0;
        for(User user : uMap.valueCollection()) {
            for(TIntIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
                if(user.getBasePid() < getUser(iter.next()).getBasePid()) {
                    count++;
                }
            }
        }
        return count;
    }

    public TIntObjectMap<TIntSet> getPartitionToUsers() {
        TIntObjectMap<TIntSet> map = new TIntObjectHashMap<>(pMap.size()+1);
        for(TIntIterator iter = getPids().iterator(); iter.hasNext(); ) {
            int pid = iter.next();
            map.put(pid, new TIntHashSet(pMap.get(pid)));
        }
        return map;
    }

    public void moveUser(Integer uid, Integer pid, boolean omitFromTally) {
        User user = getUser(uid);
        int oldPid = user.getBasePid();
        if(pMap.containsKey(oldPid)) {
            pMap.get(user.getBasePid()).remove(uid);
        }
        getPartition(pid).add(uid);
        user.setBasePid(pid);
        if(!omitFromTally) {
            increaseTally(1);
        }
    }

    public TIntSet getPids() {
        return pMap.keySet();
    }

    public TIntObjectMap<TIntSet> getFriendships() {
        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>(getNumPartitions() + 1);
        for(Integer uid : uMap.keys()) {
            friendships.put(uid, new TIntHashSet(getUser(uid).getFriendIDs()));
        }
        return friendships;
    }

    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * logicalMigrationTally);
    }

    void increaseTally(int amount) {
        migrationTally += amount;
    }

    public void increaseTallyLogical(int amount) {
        logicalMigrationTally += amount;
    }

    @Override
    public String toString() {
        return "#U:" + getNumUsers() + "|#P:" + getNumPartitions();
    }

    public void checkValidity() {
        for(Integer uid : uMap.keys()) {
            Integer observedMasterPid = null;
            for(Integer pid : pMap.keys()) {
                if(pMap.get(pid).contains(uid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot be in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must be in some partition");
            }
            if(!observedMasterPid.equals(getUser(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }
            if(!observedMasterPid.equals(uMap.get(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's PID and system's");
            }
        }
    }

    public Integer getPidForUser(Integer uid) {
        return uMap.get(uid).getBasePid();
    }

}
