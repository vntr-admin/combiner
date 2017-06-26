package io.vntr.manager;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.User;

import static io.vntr.utils.TroveUtils.getRandomElement;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class NoRepManager {

    private TShortObjectMap<User> uMap;
    private TShortObjectMap<TShortSet> pMap;

    private final boolean placeNewUserRandomly;

    private long migrationTally;
    private long logicalMigrationTally;
    private final double logicalMigrationRatio;

    private short nextPid = 1;
    private short nextUid = 1;

    public NoRepManager(double logicalMigrationRatio, boolean placeNewUserRandomly) {
        this.placeNewUserRandomly = placeNewUserRandomly;
        this.logicalMigrationRatio = logicalMigrationRatio;
        uMap = new TShortObjectHashMap<>();
        pMap = new TShortObjectHashMap<>();
    }

    public TShortSet getUids() {
        return uMap.keySet();
    }

    public User getUser(Short uid) {
        return uMap.get(uid);
    }

    public TShortSet getPartition(Short pid) {
        return pMap.get(pid);
    }

    public short addUser() {
        short newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        if(user.getBasePid() == -1) {
            user.setBasePid(getInitialPid());
        }
        uMap.put(user.getId(), user);
        pMap.get(user.getBasePid()).add(user.getId());
        if(user.getId() >= nextUid) {
            nextUid = (short)(user.getId() + 1);
        }
    }

    public void removeUser(short uid) {
        TShortSet friendIds = new TShortHashSet(getUser(uid).getFriendIDs());
        for(TShortIterator iter = friendIds.iterator(); iter.hasNext(); ) {
            unfriend(uid, iter.next());
        }
        getPartition(getPidForUser(uid)).remove(uid);
        uMap.remove(uid);
    }

    public void befriend(short id1, short id2) {
        getUser(id1).befriend(id2);
        getUser(id2).befriend(id1);
    }

    public void unfriend(short id1, short id2) {
        getUser(id1).unfriend(id2);
        getUser(id2).unfriend(id1);
    }

    short getInitialPid() {
        if(!placeNewUserRandomly) {
            int minUsers = Integer.MAX_VALUE;
            Short minPartition = null;
            for(short pid : pMap.keys()) {
                if(pMap.get(pid).size() < minUsers) {
                    minUsers = pMap.get(pid).size();
                    minPartition = pid;
                }
            }
            if(minPartition != null) {
                return minPartition;
            }
        }
        return getRandomElement(pMap.keySet());
    }

    public short addPartition() {
        short pid = nextPid;
        addPartition(pid);
        return pid;
    }

    public void addPartition(short pid) {
        pMap.put(pid, new TShortHashSet());
        if(pid >= nextPid) {
            nextPid = (short)(pid + 1);
        }
    }

    public void removePartition(short pid) {
        pMap.remove(pid);
    }

    public short getNumUsers() {
        return (short) uMap.size();
    }

    public short getNumPartitions() {
        return (short) pMap.size();
    }

    public Integer getEdgeCut() {
        int count = 0;
        for(User user : uMap.valueCollection()) {
            for(TShortIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
                if(user.getBasePid() < getUser(iter.next()).getBasePid()) {
                    count++;
                }
            }
        }
        return count;
    }

    public TShortObjectMap<TShortSet> getPartitionToUsers() {
        TShortObjectMap<TShortSet> map = new TShortObjectHashMap<>(pMap.size()+1);
        for(TShortIterator iter = getPids().iterator(); iter.hasNext(); ) {
            short pid = iter.next();
            map.put(pid, new TShortHashSet(pMap.get(pid)));
        }
        return map;
    }

    public void moveUser(short uid, short pid, boolean omitFromTally) {
        User user = getUser(uid);
        short oldPid = user.getBasePid();
        if(pMap.containsKey(oldPid)) {
            pMap.get(user.getBasePid()).remove(uid);
        }
        getPartition(pid).add(uid);
        user.setBasePid(pid);
        if(!omitFromTally) {
            increaseTally(1);
        }
    }

    public TShortSet getPids() {
        return pMap.keySet();
    }

    public TShortObjectMap<TShortSet> getFriendships() {
        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>(getNumPartitions() + 1);
        for(short uid : uMap.keys()) {
            friendships.put(uid, new TShortHashSet(getUser(uid).getFriendIDs()));
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
        for(short uid : uMap.keys()) {
            Short observedMasterPid = null;
            for(short pid : pMap.keys()) {
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

    public short getPidForUser(short uid) {
        return uMap.get(uid).getBasePid();
    }

}
