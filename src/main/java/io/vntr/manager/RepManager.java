package io.vntr.manager;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.User;

import static io.vntr.utils.TroveUtils.*;

public class RepManager {
    private final int minNumReplicas;

    private int nextPid = 1;
    private int nextUid = 1;

    private long migrationTally;
    private long logicalMigrationTally;
    private final double logicalMigrationRatio;

    private TIntObjectMap<Partition> pMap;
    private TIntIntMap uMap;

    public RepManager(int minNumReplicas, double logicalMigrationRatio) {
        this.minNumReplicas = minNumReplicas;
        this.logicalMigrationRatio = logicalMigrationRatio;
        pMap = new TIntObjectHashMap<>();
        uMap = new TIntIntHashMap();
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    Partition getPartitionById(Integer id) {
        return pMap.get(id);
    }

    public RepUser getUserMaster(Integer id) {
        Partition partition = getPartitionById(uMap.get(id));
        if (partition != null) {
            return partition.getMasterById(id);
        }
        return null;
    }

    public int getNumUsers() {
        return uMap.size();
    }

    public TIntSet getPids() {
        return pMap.keySet();
    }

    public TIntSet getUids() {
        return uMap.keySet();
    }

    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer masterPartitionId = getPidWithFewestMasters();

        RepUser RepUser = new RepUser(user.getId(), masterPartitionId);

        addUser(RepUser, masterPartitionId);

        for(TIntIterator iter = getPartitionsToAddInitialReplicas(masterPartitionId).iterator(); iter.hasNext(); ) {
            addReplica(RepUser, iter.next());
        }
    }

    public void addUser(RepUser user, Integer masterPartitionId) {
        int uid = user.getId();
        getPartitionById(masterPartitionId).addMaster(user);
        uMap.put(uid, masterPartitionId);
        if(uid >= nextUid) {
            nextUid = uid + 1;
        }
    }

    public void removeUser(Integer uid) {
        RepUser user = getUserMaster(uid);

        //Remove user from relevant partitions
        getPartitionById(user.getBasePid()).removeMaster(uid);
        for(TIntIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            getPartitionById(iter.next()).removeReplica(uid);
        }

        //Remove user from uMap
        uMap.remove(uid);

        //Remove friendships
        for(TIntIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            RepUser friendMaster = getUserMaster(friendId);
            friendMaster.unfriend(uid);

            for(TIntIterator iter2 = friendMaster.getReplicaPids().iterator(); iter2.hasNext(); ) {
                int friendReplicaPartitionId = iter2.next();
                Partition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(uid);
            }
        }
    }

    public Integer addPartition() {
        Integer newId = nextPid;
        addPartition(newId);
        return newId;
    }

    public void addPartition(Integer pid) {
        pMap.put(pid, new Partition(pid));
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    public void removePartition(Integer id) {
        pMap.remove(id);
    }

    public void addReplica(RepUser user, Integer destPid) {
        RepUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for(TIntIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    public RepUser addReplicaNoUpdates(RepUser user, Integer destPid) {
        RepUser replica = user.dupe();
        pMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(RepUser user, Integer removalPid) {
        //Delete it from each replica's replicaPartitionIds
        for(TIntIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            Partition p = pMap.get(iter.next());
            RepUser r = p.getReplicaById(user.getId());
            r.removeReplicaPartitionId(removalPid);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPid);

        //Actually remove the replica from the partition itself
        pMap.get(removalPid).removeReplica(user.getId());
    }

    public void moveUser(RepUser user, Integer toPid, TIntSet replicateInDestinationPartition, TIntSet replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        Integer uid = user.getId();
        Integer fromPid = user.getBasePid();

        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for(TIntIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
            if (uMap.get(iter.next()) == fromPid) {
                addReplica(user, fromPid);
                break;
            }
        }

        for(TIntIterator iter = replicateInDestinationPartition.iterator(); iter.hasNext(); ) {
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
        for(TIntIterator iter = replicasToDeleteInSourcePartition.iterator(); iter.hasNext(); ) {
            removeReplica(getUserMaster(iter.next()), fromPid);
        }

        increaseTally(1);
    }

    public void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        RepUser user = getUserMaster(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        uMap.put(uid, toPid);

        user.setBasePid(toPid);

        for(TIntIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(uid).setBasePid(toPid);
        }
    }

    public void befriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for(TIntIterator iter = smallerUser.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }
        for(TIntIterator iter = largerUser.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for(TIntIterator iter = smallerUser.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }
        for(TIntIterator iter = largerUser.getReplicaPids().iterator(); iter.hasNext(); ) {
            pMap.get(iter.next()).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        Partition partition = pMap.get(partitionId);
        RepUser user = partition.getReplicaById(userId);
        user.setBasePid(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        uMap.put(userId, partitionId);

        for(TIntIterator iter = user.getReplicaPids().iterator(); iter.hasNext(); ) {
            RepUser replica = pMap.get(iter.next()).getReplicaById(userId);
            replica.setBasePid(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }

        //Add replicas of friends in pid if they don't already exists
        for(TIntIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            if(!partition.getIdsOfMasters().contains(friendId) && !partition.getIdsOfReplicas().contains(friendId)) {
                addReplica(getUserMaster(friendId), partitionId);
            }
        }
    }

    public Integer getPidWithFewestMasters() {
        int minMasters = Integer.MAX_VALUE;
        Integer minId = -1;

        for (Integer id : pMap.keys()) {
            int numMasters = getPartitionById(id).getNumMasters();
            if (numMasters < minMasters) {
                minMasters = numMasters;
                minId = id;
            }
        }

        return minId;
    }

    public Integer getRandomPidWhereThisUserIsNotPresent(RepUser user) {
        TIntSet potentialReplicaLocations = new TIntHashSet(pMap.keySet());
        potentialReplicaLocations.remove(user.getBasePid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        int[] array = potentialReplicaLocations.toArray();
        return array[(int)(array.length * Math.random())];
    }

    public TIntSet getPartitionsToAddInitialReplicas(Integer masterPid) {
        int[] pidsMinusMasterPid = removeUniqueElementFromNonEmptyArray(pMap.keySet().toArray(), masterPid);
        return getKDistinctValuesFromArray(getMinNumReplicas(), pidsMinusMasterPid);
    }

    public TIntObjectMap<TIntSet> getPartitionToUserMap() {
        TIntObjectMap<TIntSet> map = new TIntObjectHashMap<>(pMap.size() + 1);
        for (Integer pid : pMap.keys()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    public TIntObjectMap<TIntSet> getPartitionToReplicasMap() {
        TIntObjectMap<TIntSet> map = new TIntObjectHashMap<>(pMap.size() + 1);
        for (Integer pid : pMap.keys()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    public Integer getEdgeCut() {
        int count = 0;
        for (Integer uid : uMap.keys()) {
            RepUser user = getUserMaster(uid);
            Integer pid = user.getBasePid();
            for(TIntIterator iter = user.getFriendIDs().iterator(); iter.hasNext(); ) {
                if (pid < uMap.get(iter.next())) {
                    count++;
                }
            }
        }
        return count;
    }

    public Integer getReplicationCount() {
        int count = 0;
        for(TIntIterator iter = getPids().iterator(); iter.hasNext(); ) {
            count += getPartitionById(iter.next()).getNumReplicas();
        }
        return count;
    }

    public TIntObjectMap<TIntSet> getFriendships() {
        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>(uMap.size()+1);
        for(Integer uid : uMap.keys()) {
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
        for(Integer uid : uMap.keys()) {
            Integer observedMasterPid = null;
            for(Integer pid : pMap.keys()) {
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
        for(Integer uid : uMap.keys()) {
            RepUser user = getUserMaster(uid);
            TIntSet observedReplicaPids = new TIntHashSet();
            for(Integer pid : pMap.keys()) {
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
        TIntObjectMap<TIntSet> friendships = getFriendships();
        TIntObjectMap<TIntSet> partitions  = getPartitionToUserMap();
        TIntObjectMap<TIntSet> replicas    = getPartitionToReplicasMap();
        for(int uid1 : friendships.keys()) {
            for(TIntIterator iter = friendships.get(uid1).iterator(); iter.hasNext(); ) {
                int uid2 = iter.next();
                TIntSet uid1Replicas = findKeysForUser(replicas, uid1);
                TIntSet uid2Replicas = findKeysForUser(replicas, uid2);
                int pid1 = findKeysForUser(partitions, uid1).iterator().next();
                int pid2 = findKeysForUser(partitions, uid2).iterator().next();
                if(pid1 != pid2) {
                    //If they aren't colocated, they have replicas in each other's partitions
                    valid &= uid1Replicas.contains(pid2);
                    valid &= uid2Replicas.contains(pid1);
                }
            }
        }
        return valid;
    }

    private static TIntSet findKeysForUser(TIntObjectMap<TIntSet> m, int uid) {
        TIntSet keys = new TIntHashSet();
        for(int key : m.keys()) {
            if(m.get(key).contains(uid)) {
                keys.add(key);
            }
        }
        return keys;
    }

    public TIntSet getMastersOnPartition(int pid) {
        return getPartitionById(pid).getIdsOfMasters();
    }

    public TIntSet getReplicasOnPartition(int pid) {
        return getPartitionById(pid).getIdsOfReplicas();
    }

    public RepUser getReplicaOnPartition(int uid, int pid) {
        return getPartitionById(pid).getReplicaById(uid);
    }

    static class Partition {
        private TIntObjectMap<RepUser> idToMasterMap = new TIntObjectHashMap<>();
        private TIntObjectMap<RepUser> idToReplicaMap = new TIntObjectHashMap<>();
        private final Integer id;

        Partition(Integer id) {
            this.id = id;
        }

        void addMaster(RepUser user) {
            idToMasterMap.put(user.getId(), user);
        }

        User removeMaster(Integer id) {
            return idToMasterMap.remove(id);
        }

        void addReplica(RepUser user) {
            idToReplicaMap.put(user.getId(), user);
        }

        User removeReplica(Integer id) {
            return idToReplicaMap.remove(id);
        }

        RepUser getMasterById(Integer userId) {
            return idToMasterMap.get(userId);
        }

        RepUser getReplicaById(Integer userId) {
            return idToReplicaMap.get(userId);
        }

        int getNumMasters() {
            return idToMasterMap.size();
        }

        int getNumReplicas() {
            return idToReplicaMap.size();
        }

        TIntSet getIdsOfMasters() {
            return idToMasterMap.keySet();
        }

        public TIntSet getIdsOfReplicas() {
            return idToReplicaMap.keySet();
        }

        public Integer getId() {
            return id;
        }

    }
}
