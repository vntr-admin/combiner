package io.vntr.sparmes;

import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.repartition.SparmesRepartitioner;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.Utils.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesManager {
    private int minNumReplicas;
    private float gamma;
    private boolean probabilistic;
    private int k;

    private int nextUid = 1;
    private int nextPid = 1;

    private long migrationTally;
    private long migrationTallyLogical;
    private double logicalMigrationRatio;

    private Map<Integer, SparmesPartition> pMap;

    private Map<Integer, Integer> uMap = new HashMap<>();

    private SparmesBefriendingStrategy sparmesBefriendingStrategy;

    public SparmesManager(int minNumReplicas, float gamma, int k, boolean probabilistic) {
        this.minNumReplicas = minNumReplicas;
        this.gamma = gamma;
        this.k = k;
        this.probabilistic = probabilistic;
        this.pMap = new HashMap<>();
        this.sparmesBefriendingStrategy = new SparmesBefriendingStrategy(this);
        this.logicalMigrationRatio = 0;
    }

    public SparmesManager(int minNumReplicas, float gamma, int k, boolean probabilistic, double logicalMigrationRatio) {
        this.minNumReplicas = minNumReplicas;
        this.gamma = gamma;
        this.k = k;
        this.probabilistic = probabilistic;
        this.pMap = new HashMap<>();
        this.sparmesBefriendingStrategy = new SparmesBefriendingStrategy(this);
        this.logicalMigrationRatio = logicalMigrationRatio;
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public SparmesBefriendingStrategy getSparmesBefriendingStrategy() {
        return sparmesBefriendingStrategy;
    }

    public SparmesPartition getPartitionById(Integer id) {
        return pMap.get(id);
    }

    public RepUser getUserMasterById(Integer id) {
        Integer partitionId = uMap.get(id);
        if (partitionId != null) {
            SparmesPartition partition = getPartitionById(partitionId);
            if (partition != null) {
                return partition.getMasterById(id);
            }
        }
        return null;
    }

    public int getNumUsers() {
        return uMap.size();
    }

    public Set<Integer> getPids() {
        return pMap.keySet();
    }

    public Set<Integer> getUids() {
        return uMap.keySet();
    }

    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer masterPartitionId = getPartitionIdWithFewestMasters();

        RepUser RepUser = new RepUser(user.getId(), masterPartitionId);//, gamma, this, minNumReplicas);

        addUser(RepUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(RepUser, id);
        }
    }

    void addUser(RepUser user, Integer masterPartitionId) {
        int uid = user.getId();
        getPartitionById(masterPartitionId).addMaster(user);
        uMap.put(uid, masterPartitionId);
        if(uid >= nextUid) {
            nextUid = uid + 1;
        }
    }

    public void removeUser(Integer userId) {
        RepUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getBasePid()).removeMaster(userId);
        for (Integer replicaPartitionId : user.getReplicaPids()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from uMap
        uMap.remove(userId);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            RepUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Integer friendReplicaPartitionId : friendMaster.getReplicaPids()) {
                SparmesPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(userId);
            }
        }
    }

    public Integer addPartition() {
        Integer newId = nextPid;
        addPartition(newId);
        return newId;
    }

    void addPartition(Integer pid) {
        pMap.put(pid, new SparmesPartition(pid));//, gamma, this));
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
        for (Integer pid : user.getReplicaPids()) {
            pMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    RepUser addReplicaNoUpdates(RepUser user, Integer destPid) {
        RepUser replica = user.dupe();
        pMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(RepUser user, Integer removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
            pMap.get(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        pMap.get(removalPartitionId).removeReplica(user.getId());

    }

    public void befriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPids()) {
            pMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPids()) {
            pMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPids()) {
            pMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPids()) {
            pMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void moveUser(RepUser user, Integer toPid, Set<Integer> replicateInDestinationPartition, Set<Integer> replicasToDeleteInSourcePartition) {
        Integer uid = user.getId();
        Integer fromPid = user.getBasePid();

        //Step 1: move the actual user
        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for (Integer friendId : user.getFriendIDs()) {
            if (uMap.get(friendId).equals(fromPid)) {
                addReplica(user, fromPid);
                break;
            }
        }

        for (Integer friendToReplicateId : replicateInDestinationPartition) {
            addReplica(getUserMasterById(friendToReplicateId), toPid);
        }

        //Step 3: remove unnecessary replicas
        //Possibilities:
        // (1) replica of user in destinationPartition
        // (2) replicas of user's friends in oldPartition with no other purpose
        // (3) [the replica of the new friend that prompted this move should already be accounted for in (2)]

        if (user.getReplicaPids().contains(toPid)) {
            if (user.getReplicaPids().size() <= minNumReplicas) {
                //add one in another partition that doesn't yet have one of this user
                addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user));
            }
            removeReplica(user, toPid);
        }

        //delete the replica of the appropriate friends in oldPartition
        for (Integer replicaIdToDelete : replicasToDeleteInSourcePartition) {
            removeReplica(getUserMasterById(replicaIdToDelete), fromPid);
        }
    }

    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        SparmesPartition partition = pMap.get(partitionId);
        RepUser user = partition.getReplicaById(userId);
        user.setBasePid(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        uMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPids()) {
            RepUser replica = pMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setBasePid(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }

        //Add replicas of friends in pid if they don't already exist
        for(int friendId : user.getFriendIDs()) {
            if (!partition.getIdsOfMasters().contains(friendId) && !partition.getIdsOfReplicas().contains(friendId)) {
                addReplica(getUserMasterById(friendId), partitionId);
            }
        }
    }

    Integer getPartitionIdWithFewestMasters() {
        int minMasters = Integer.MAX_VALUE;
        Integer minId = -1;

        for (Integer id : pMap.keySet()) {
            int numMasters = getPartitionById(id).getNumMasters();
            if (numMasters < minMasters) {
                minMasters = numMasters;
                minId = id;
            }
        }

        return minId;
    }

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(RepUser user) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(pMap.keySet());
        potentialReplicaLocations.remove(user.getBasePid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        List<Integer> list = new LinkedList<>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    Set<Integer> getPartitionsToAddInitialReplicas(Integer masterPartitionId) {
        List<Integer> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<>(pMap.keySet());
        partitionIdsAtWhichReplicasCanBeAdded.remove(masterPartitionId);
        return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
    }

    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : pMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    Map<Integer, Set<Integer>> getPartitionToReplicasMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : pMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    public Integer getEdgeCut() {
        int count = 0;
        for (Integer uid : uMap.keySet()) {
            RepUser user = getUserMasterById(uid);
            Integer pid = user.getBasePid();
            for (Integer friendId : user.getFriendIDs()) {
                if (!pid.equals(uMap.get(friendId))) {
                    count++;
                }
            }
        }
        return count / 2;
    }

    public Integer getReplicationCount() {
        int count = 0;
        for(Integer pid : getPids()) {
            count += getPartitionById(pid).getNumReplicas();
        }
        return count;
    }

    void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        RepUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        uMap.put(uid, toPid);

        user.setBasePid(toPid);

        for (Integer rPid : user.getReplicaPids()) {
            pMap.get(rPid).getReplicaById(uid).setBasePid(toPid);
        }
    }

    public void reformedRepartition() {
        SparmesRepartitioner.Results results = SparmesRepartitioner.repartition(k, 100, gamma, minNumReplicas, getPartitionToUserMap(), getPartitionToReplicasMap(), getFriendships());
        increaseTallyLogical(results.getNumLogicalMoves());
        physicallyMigrate(results);
    }

    void physicallyMigrate(SparmesRepartitioner.Results results) {
        Map<Integer, Integer> currentUidToPidMap = getUToMasterMap(getPartitionToUserMap());
        Map<Integer, Set<Integer>> currentUidToReplicasMap = getUToReplicasMap(getPartitionToReplicasMap(), getUids());

        for(int uid : getUids()) {
            int currentPid = currentUidToPidMap.get(uid);
            int newPid = results.getUidsToPids().get(uid);

            //move master
            if(currentPid != newPid) {
                moveMasterAndInformReplicas(uid, currentPid, newPid);
                increaseTally(1);
            }

            //add and remove replicas as specified in the results
            Set<Integer> newReplicas = results.getUidsToReplicaPids().get(uid);
            Set<Integer> currentReplicas = currentUidToReplicasMap.get(uid);
            if(!currentReplicas.equals(newReplicas)) {
                for(int newReplica : newReplicas) {
                    if(!currentReplicas.contains(newReplica)) {
                        addReplica(getUserMasterById(uid), newReplica);
                    }
                }
                for(int oldReplica : currentReplicas) {
                    if(!newReplicas.contains(oldReplica)) {
                        removeReplica(getUserMasterById(uid), oldReplica);
                    }
                }
            }
        }

        //shore up friend replicas (ideally would be unnecessary)
        for(int uid : getUids()) {
            shoreUpFriendReplicas(uid);
        }

        //ensure k replication
        for(int uid : getUids()) {
            ensureKReplication(uid);
        }
    }

    void shoreUpFriendReplicas(int uid) {
        RepUser user = getUserMasterById(uid);
        int pid = user.getBasePid();
        SparmesPartition partition = getPartitionById(pid);
        Set<Integer> friends = new HashSet<>(user.getFriendIDs());
        friends.removeAll(partition.getIdsOfMasters());
        friends.removeAll(partition.getIdsOfReplicas());
        for(int friendId : friends) {
            addReplica(getUserMasterById(friendId), pid);
        }
    }

    void ensureKReplication(int uid) {
        RepUser user = getUserMasterById(uid);
        Set<Integer> replicaLocations = user.getReplicaPids();
        int deficit = minNumReplicas - replicaLocations.size();
        if(deficit > 0) {
            Set<Integer> newLocations = new HashSet<>(pMap.keySet());
            newLocations.removeAll(replicaLocations);
            newLocations.remove(user.getBasePid());
            for(int rPid : ProbabilityUtils.getKDistinctValuesFromList(deficit, newLocations)) {
                addReplica(user, rPid);
            }
        }
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : uMap.keySet()) {
            friendships.put(uid, getUserMasterById(uid).getFriendIDs());
        }
        return friendships;
    }

    private static Set<Integer> findKeysForUser(Map<Integer, Set<Integer>> m, int uid) {
        Set<Integer> keys = new HashSet<>();
        for(int key : m.keySet()) {
            if(m.get(key).contains(uid)) {
                keys.add(key);
            }
        }
        return keys;
    }

    public long getMigrationTally() {
        return migrationTally + (long) (logicalMigrationRatio * migrationTallyLogical);
    }

    void increaseTally(int amount) {
        migrationTally += amount;
    }

    void increaseTallyLogical(int amount) {
        migrationTallyLogical += amount;
    }

    void checkValidity() {

        //Check masters
        for(Integer uid : uMap.keySet()) {
            Integer observedMasterPid = null;
            for(Integer pid : pMap.keySet()) {
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
            if(!observedMasterPid.equals(getUserMasterById(uid).getBasePid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }

            //TODO: should we check the logical partitions?
        }

        //check replicas
        for(Integer uid : uMap.keySet()) {
            RepUser user = getUserMasterById(uid);
            Set<Integer> observedReplicaPids = new HashSet<>();
            for(Integer pid : pMap.keySet()) {
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
        Map<Integer, Set<Integer>> friendships = getFriendships();
        Map<Integer, Set<Integer>> partitions  = getPartitionToUserMap();
        Map<Integer, Set<Integer>> replicas    = getPartitionToReplicasMap();
        for(int uid1 : friendships.keySet()) {
            for(int uid2 : friendships.get(uid1)) {
                Set<Integer> uid1Replicas = findKeysForUser(replicas, uid1);
                Set<Integer> uid2Replicas = findKeysForUser(replicas, uid2);
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

    @Override
    public String toString() {
        return "minNumReplicas:" + minNumReplicas + "|gamma:" + gamma + "|probabilistic:" + probabilistic + "|#U:" + getNumUsers() + "|#P:" + pMap.size();
    }
}
