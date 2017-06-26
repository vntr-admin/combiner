package io.vntr.manager;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.RepUser;
import io.vntr.User;

import static io.vntr.utils.TroveUtils.*;

public class RepManager {
    private final short minNumReplicas;

    private short nextPid = 1;
    private short nextUid = 1;

    private long migrationTally;
    private long logicalMigrationTally;
    private final double logicalMigrationRatio;

    private TShortObjectMap<Partition> pMap;
    private TShortShortMap uMap;

    public RepManager(short minNumReplicas, double logicalMigrationRatio) {
        this.minNumReplicas = minNumReplicas;
        this.logicalMigrationRatio = logicalMigrationRatio;
        pMap = new TShortObjectHashMap<>();
        uMap = new TShortShortHashMap();
    }

    public short getMinNumReplicas() {
        return minNumReplicas;
    }

    Partition getPartitionById(short id) {
        return pMap.get(id);
    }

    public RepUser getUserMaster(short id) {
        Partition partition = getPartitionById(uMap.get(id));
        if (partition != null) {
            return partition.getMasterById(id);
        }
        return null;
    }

    public short getNumUsers() {
        return (short) uMap.size();
    }

    public TShortSet getPids() {
        return pMap.keySet();
    }

    public TShortSet getUids() {
        return uMap.keySet();
    }

    public short addUser() {
        short newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        short masterPid = getPidWithFewestMasters();

        RepUser RepUser = new RepUser(user.getId(), masterPid);

        addUser(RepUser, masterPid);

        for(TShortIterator iter = getPartitionsToAddInitialReplicas(masterPid).iterator(); iter.hasNext(); ) {
            addReplica(RepUser, iter.next());
        }
    }

    public void addUser(RepUser user, short masterPid) {
        short uid = user.getId();
        getPartitionById(masterPid).addMaster(user);
        uMap.put(uid, masterPid);
        if(uid >= nextUid) {
            nextUid = (short) (uid + 1);
        }
    }

    public void removeUser(short uid) {
        RepUser user = getUserMaster(uid);

        //Remove user from relevant partitions
        getPartitionById(user.getBasePid()).removeMaster(uid);
        for(TShortIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            getPartitionById(iter.next()).removeReplica(uid);
        }

        //Remove user from uMap
        uMap.remove(uid);

        //Remove friendships
        for(TShortIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            RepUser friendMaster = getUserMaster(friendId);
            friendMaster.unfriend(uid);

            for(TShortIterator iter2 = friendMaster.getReplicaPids().iterator(); iter2.hasNext(); ) {
                short friendReplicaPid = iter2.next();
                Partition friendReplicaPartition = getPartitionById(friendReplicaPid);
                friendReplicaPartition.getReplicaById(friendId).unfriend(uid);
            }
        }
    }

    public short addPartition() {
        short newId = nextPid;
        addPartition(newId);
        return newId;
    }

    public void addPartition(short pid) {
        pMap.put(pid, new Partition(pid));
        if(pid >= nextPid) {
            nextPid = (short)(pid + 1);
        }
    }

    public void removePartition(short id) {
        pMap.remove(id);
    }

    public void addReplica(RepUser user, short destPid) {
        RepUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPids to reflect this addition
        replicaOfUser.addReplicaPids(destPid);
        for(TShortIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(user.getId()).addReplicaPids(destPid);
        }
        user.addReplicaPids(destPid);
    }

    public RepUser addReplicaNoUpdates(RepUser user, short destPid) {
        RepUser replica = user.dupe();
        pMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(RepUser user, short removalPid) {
        //Delete it from each replica's replicaPids
        for(TShortIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            Partition p = pMap.get(iter.next());
            RepUser r = p.getReplicaById(user.getId());
            r.removeReplicaPid(removalPid);
        }

        //Delete it from the master's replicaPids
        user.removeReplicaPid(removalPid);

        //Actually remove the replica from the partition itself
        pMap.get(removalPid).removeReplica(user.getId());
    }

    public void moveUser(RepUser user, short toPid, TShortSet replicateInDestinationPartition, TShortSet replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        short uid = user.getId();
        short fromPid = user.getBasePid();

        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for(TShortIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
            if (uMap.get(iter.next()) == fromPid) {
                addReplica(user, fromPid);
                break;
            }
        }

        for(TShortIterator iter = replicateInDestinationPartition.iterator(); iter.hasNext(); ) {
            addReplica(getUserMaster(iter.next()), toPid);
        }

        //Step 3: remove unnecessary replicas
        //Possibilities:
        // (1) replica of user in destinationPartition
        // (2) replicas of user's friends in oldPartition with no other purpose
        // (3) [the replica of the new friend that prompted this move should already be accounted for in (2)]


        if (user.getReplicaPids().contains(toPid)) {
            if (user.getReplicaPids().size() <= minNumReplicas) {
                //add one in another partition that doesn't yet have one of this user
                addReplica(user, getRandomPidWhereThisUserIsNotPresent(user));
            }
            removeReplica(user, toPid);
        }

        //delete the replica of the appropriate friends in oldPartition
        for(TShortIterator iter = replicasToDeleteInSourcePartition.iterator(); iter.hasNext(); ) {
            removeReplica(getUserMaster(iter.next()), fromPid);
        }

        increaseTally(1);
    }

    public void moveMasterAndInformReplicas(short uid, short fromPid, short toPid) {
        RepUser user = getUserMaster(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        uMap.put(uid, toPid);

        user.setBasePid(toPid);

        for(TShortIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(uid).setBasePid(toPid);
        }
    }

    public void befriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for(TShortIterator iter = smallerUser.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }
        for(TShortIterator iter = largerUser.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for(TShortIterator iter = smallerUser.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }
        for(TShortIterator iter = largerUser.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void promoteReplicaToMaster(short uid, short pid) {
        Partition partition = pMap.get(pid);
        RepUser user = partition.getReplicaById(uid);
        user.setBasePid(pid);
        user.removeReplicaPid(pid);
        partition.addMaster(user);
        partition.removeReplica(uid);

        uMap.put(uid, pid);

        for(TShortIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = pMap.get(iter.next()).getReplicaById(uid);
            replica.setBasePid(pid);
            replica.removeReplicaPid(pid);
        }

        //Add replicas of friends in pid if they don't already exists
        for(TShortIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            if(!partition.getIdsOfMasters().contains(friendId) && !partition.getIdsOfReplicas().contains(friendId)) {
                addReplica(getUserMaster(friendId), pid);
            }
        }
    }

    public short getPidWithFewestMasters() {
        int minMasters = Integer.MAX_VALUE;
        short minId = -1;

        for (short id : pMap.keys()) {
            int numMasters = getPartitionById(id).getNumMasters();
            if (numMasters < minMasters) {
                minMasters = numMasters;
                minId = id;
            }
        }

        return minId;
    }

    public short getRandomPidWhereThisUserIsNotPresent(RepUser user) {
        TShortSet potentialReplicaLocations = new TShortHashSet(pMap.keySet());
        potentialReplicaLocations.remove(user.getBasePid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        short[] array = potentialReplicaLocations.toArray();
        return array[(int)(array.length * Math.random())];
    }

    public TShortSet getPartitionsToAddInitialReplicas(short masterPid) {
        short[] pidsMinusMasterPid = removeUniqueElementFromNonEmptyArray(pMap.keySet().toArray(), masterPid);
        return getKDistinctValuesFromArray(getMinNumReplicas(), pidsMinusMasterPid);
    }

    public TShortObjectMap<TShortSet> getPartitionToUserMap() {
        TShortObjectMap<TShortSet> map = new TShortObjectHashMap<>(pMap.size() + 1);
        for (short pid : pMap.keys()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    public TShortObjectMap<TShortSet> getPartitionToReplicasMap() {
        TShortObjectMap<TShortSet> map = new TShortObjectHashMap<>(pMap.size() + 1);
        for (short pid : pMap.keys()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    public Integer getEdgeCut() {
        int count = 0;
        for (short uid : uMap.keys()) {
            RepUser user = getUserMaster(uid);
            short pid = user.getBasePid();
            for(TShortIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
                if (pid < uMap.get(iter.next())) {
                    count++;
                }
            }
        }
        return count;
    }

    public Integer getReplicationCount() {
        int count = 0;
        for(TShortIterator iter = getPids().iterator(); iter.hasNext(); ) {
            count += getPartitionById(iter.next()).getNumReplicas();
        }
        return count;
    }

    public TShortObjectMap<TShortSet> getFriendships() {
        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>(uMap.size()+1);
        for(short uid : uMap.keys()) {
            friendships.put(uid, getUserMaster(uid).getFriendIDs());
        }
        return friendships;
    }

    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * logicalMigrationTally);
    }

    public void increaseTally(int amount) {
        migrationTally += amount;
    }

    public void increaseTallyLogical(int amount) {
        logicalMigrationTally += amount;
    }

    @Override
    public String toString() {
        return "minNumReplicas:" + minNumReplicas + "|#U:" + getNumUsers() + "|#P:" + pMap.size();
    }

    public void checkValidity() {

        //Check masters
        for(short uid : uMap.keys()) {
            Short observedMasterPid = null;
            for(short pid : pMap.keys()) {
                if(pMap.get(pid).getIdsOfMasters().contains(uid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot be in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must be in some partition");
            }
            if(!observedMasterPid.equals(uMap.get(uid))) {
                throw new RuntimeException("Mismatch between uMap's location of user and partition's");
            }
            if(!observedMasterPid.equals(getUserMaster(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }

            //TODO: should we check the logical partitions?
        }

        //check replicas
        for(short uid : uMap.keys()) {
            RepUser user = getUserMaster(uid);
            TShortSet observedReplicaPids = new TShortHashSet(pMap.size()+1);
            for(short pid : pMap.keys()) {
                if(pMap.get(pid).getIdsOfReplicas().contains(uid)) {
                    observedReplicaPids.add(pid);
                }
            }
            if(observedReplicaPids.size() < minNumReplicas) {
                throw new RuntimeException("Insufficient replicas");
            }
            if(!observedReplicaPids.equals(user.getReplicaPids())) {
                throw new RuntimeException("Mismatch between user's replica PIDs and system's");
            }
        }

        //check local semantics
        if(!checkLocalSemantics()) {
            throw new RuntimeException("local semantics issue!");
        }
    }

    private boolean checkLocalSemantics() {
        boolean valid = true;
        TShortObjectMap<TShortSet> friendships = getFriendships();
        TShortObjectMap<TShortSet> partitions  = getPartitionToUserMap();
        TShortObjectMap<TShortSet> replicas    = getPartitionToReplicasMap();
        for(short uid1 : friendships.keys()) {
            for(TShortIterator iter = friendships.get(uid1).iterator(); iter.hasNext(); ) {
                short uid2 = iter.next();
                TShortSet uid1Replicas = findKeysForUser(replicas, uid1);
                TShortSet uid2Replicas = findKeysForUser(replicas, uid2);
                short pid1 = findKeysForUser(partitions, uid1).iterator().next();
                short pid2 = findKeysForUser(partitions, uid2).iterator().next();
                if(pid1 != pid2) {
                    //If they aren't colocated, they have replicas in each other's partitions
                    valid &= uid1Replicas.contains(pid2);
                    valid &= uid2Replicas.contains(pid1);
                }
            }
        }
        return valid;
    }

    private static TShortSet findKeysForUser(TShortObjectMap<TShortSet> m, short uid) {
        TShortSet keys = new TShortHashSet();
        for(short key : m.keys()) {
            if(m.get(key).contains(uid)) {
                keys.add(key);
            }
        }
        return keys;
    }

    public TShortSet getMastersOnPartition(short pid) {
        return getPartitionById(pid).getIdsOfMasters();
    }

    public TShortSet getReplicasOnPartition(short pid) {
        return getPartitionById(pid).getIdsOfReplicas();
    }

    public RepUser getReplicaOnPartition(short uid, short pid) {
        return getPartitionById(pid).getReplicaById(uid);
    }

    static class Partition {
        private TShortObjectMap<RepUser> idToMasterMap = new TShortObjectHashMap<>();
        private TShortObjectMap<RepUser> idToReplicaMap = new TShortObjectHashMap<>();
        private final short id;

        Partition(short id) {
            this.id = id;
        }

        void addMaster(RepUser user) {
            idToMasterMap.put(user.getId(), user);
        }

        User removeMaster(short id) {
            return idToMasterMap.remove(id);
        }

        void addReplica(RepUser user) {
            idToReplicaMap.put(user.getId(), user);
        }

        User removeReplica(short id) {
            return idToReplicaMap.remove(id);
        }

        RepUser getMasterById(short uid) {
            return idToMasterMap.get(uid);
        }

        RepUser getReplicaById(short uid) {
            return idToReplicaMap.get(uid);
        }

        int getNumMasters() {
            return idToMasterMap.size();
        }

        int getNumReplicas() {
            return idToReplicaMap.size();
        }

        TShortSet getIdsOfMasters() {
            return idToMasterMap.keySet();
        }

        public TShortSet getIdsOfReplicas() {
            return idToReplicaMap.keySet();
        }

        public short getId() {
            return id;
        }

    }
}
