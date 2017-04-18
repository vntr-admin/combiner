package io.vntr.sparmes;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesManager {
    private int minNumReplicas;
    private float gamma;
    private boolean probabilistic;
    private int k;

    private static final Integer defaultStartingId = 1;

    private int nextUid = 1;
    private int nextPid = 1;

    private long migrationTally;
    private long migrationTallyLogical;
    private double logicalMigrationRatio = 0;

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

    public SparmesUser getUserMasterById(Integer id) {
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

    public Set<Integer> getAllPartitionIds() {
        return pMap.keySet();
    }

    public Set<Integer> getAllUserIds() {
        return uMap.keySet();
    }


    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public void addUser(User user) {
        Integer masterPartitionId = getPartitionIdWithFewestMasters();

        SparmesUser sparmesUser = new SparmesUser(user.getId(), masterPartitionId, gamma, this, minNumReplicas);

        addUser(sparmesUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(sparmesUser, id);
        }
    }

    void addUser(SparmesUser user, Integer masterPartitionId) {
        int uid = user.getId();
        getPartitionById(masterPartitionId).addMaster(user);
        uMap.put(uid, masterPartitionId);
        if(uid >= nextUid) {
            nextUid = uid + 1;
        }
    }

    public void removeUser(Integer userId) {
        SparmesUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getMasterPid()).removeMaster(userId);
        for (Integer replicaPartitionId : user.getReplicaPids()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from uMap
        uMap.remove(userId);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            SparmesUser friendMaster = getUserMasterById(friendId);
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
        pMap.put(pid, new SparmesPartition(pid, gamma, this));
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    public void removePartition(Integer id) {
        pMap.remove(id);
    }

    public void addReplica(SparmesUser user, Integer destPid) {
        SparmesUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Integer pid : user.getReplicaPids()) {
            pMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    SparmesUser addReplicaNoUpdates(SparmesUser user, Integer destPid) {
        SparmesUser replica = user.clone();
        pMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(SparmesUser user, Integer removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
            pMap.get(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        pMap.get(removalPartitionId).removeReplica(user.getId());

    }

    public void befriend(SparmesUser smallerUser, SparmesUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPids()) {
            pMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPids()) {
            pMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(SparmesUser smallerUser, SparmesUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPids()) {
            pMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPids()) {
            pMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void moveUser(SparmesUser user, Integer toPid, Set<Integer> replicateInDestinationPartition, Set<Integer> replicasToDeleteInSourcePartition) {
        Integer uid = user.getId();
        Integer fromPid = user.getMasterPid();

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
        SparmesUser user = partition.getReplicaById(userId);
        user.setMasterPid(partitionId);
        user.setLogicalPid(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        uMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPids()) {
            SparmesUser replica = pMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setMasterPid(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }

        //Add replicas of friends in partitionId if they don't already exist
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

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SparmesUser user) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(pMap.keySet());
        potentialReplicaLocations.remove(user.getMasterPid());
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
            SparmesUser user = getUserMasterById(uid);
            Integer pid = user.getMasterPid();
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
        for(Integer pid : getAllPartitionIds()) {
            count += getPartitionById(pid).getNumReplicas();
        }
        return count;
    }

    void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        SparmesUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        uMap.put(uid, toPid);

        user.setMasterPid(toPid);
        user.setLogicalPid(toPid);

        for (Integer rPid : user.getReplicaPids()) {
            pMap.get(rPid).getReplicaById(uid).setMasterPid(toPid);
        }
    }

    public void repartition() {
        int k = 3; //TODO: set this intelligently
        for (SparmesPartition p : pMap.values()) {
            p.resetLogicalUsers();
        }

        iteration = 0;
        boolean stoppingCondition = false;
        while (!stoppingCondition) {
            boolean changed;
            changed  = performStage(true,  k);
            changed |= performStage(false, k);
            stoppingCondition = !changed;
            iteration++;
        }

        Map<Integer, Integer> usersWhoMoved = new HashMap<>();
        for (SparmesPartition p : pMap.values()) {
            Set<Integer> moved = p.physicallyCopyNewMasters();
            for(Integer uid : moved) {
                usersWhoMoved.put(uid, p.getId());
            }
        }

        uMap.putAll(usersWhoMoved);

        for (SparmesPartition p : pMap.values()) {
            p.physicallyCopyNewReplicas();
        }

        for (SparmesPartition p : pMap.values()) {
            p.physicallyMigrateDelete();
        }

        for (SparmesPartition p : pMap.values()) {
            p.shoreUpFriendReplicas();
        }

        for (int uid : uMap.keySet()) {
            ensureKReplication(uid);
        }

        increaseTally(usersWhoMoved.size());
    }

    void ensureKReplication(int uid) {
        SparmesUser user = getUserMasterById(uid);
        Set<Integer> replicaLocations = user.getReplicaPids();
        int deficit = minNumReplicas - replicaLocations.size();
        if(deficit > 0) {
            Set<Integer> newLocations = new HashSet<>(pMap.keySet());
            newLocations.removeAll(replicaLocations);
            newLocations.remove(user.getMasterPid());
            for(int rPid : ProbabilityUtils.getKDistinctValuesFromList(deficit, newLocations)) {
                addReplica(user, rPid);
            }
        }
    }

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SparmesUser user, Collection<Integer> pidsToExclude) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(getAllPartitionIds());
        potentialReplicaLocations.removeAll(pidsToExclude);
        potentialReplicaLocations.remove(user.getMasterPid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        List<Integer> list = new LinkedList<>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    static int iteration = 0;

    boolean performStage(boolean firstStage, int k) {
        if(iteration > 100) {
            return false;
        }
        boolean changed = false;
        Map<Integer, Set<Target>> stageTargets = new HashMap<>();
        for (SparmesPartition p : pMap.values()) {
            Set<Target> targets = p.getCandidates(firstStage, k, probabilistic);
            stageTargets.put(p.getId(), targets);
            changed |= !targets.isEmpty();
        }

        for(Integer pid : pMap.keySet()) {
            for(Target target : stageTargets.get(pid)) {
                migrateLogically(target);
            }
        }

        updateLogicalUsers();

        return changed;
    }

    private void addLogicalReplica(Integer uid, Integer pid) {
        getUserMasterById(uid).addLogicalPartitionId(pid);
        getPartitionById(pid).addLogicalReplicaId(uid);
    }

    private void removeLogicalReplica(Integer uid, Integer pid) {
        getUserMasterById(uid).removeLogicalPartitionId(pid);
        getPartitionById(pid).removeLogicalReplicaId(uid);
    }

    void migrateLogically(Target t) {
        SparmesPartition oldPart = getPartitionById(t.oldPid);
        SparmesPartition newPart = getPartitionById(t.newPid);
        SparmesUser user = getUserMasterById(t.uid);

        //Add the actual user
        user.setLogicalPid(t.newPid);
        oldPart.removeLogicalUser(t.uid);
        newPart.addLogicalUser(user.getLogicalUser(false));

        //Add replicas as necessary
        //First, replicate user in old partition if necessary
        for(Integer friendId : user.getFriendIDs()) {
            if(getUserMasterById(friendId).getLogicalPid().equals(t.oldPid)) {
                addLogicalReplica(t.uid, t.oldPid);
                break;
            }
        }

        //Second, replicate friends in new partition if they aren't there already
        for(Integer friendId : user.getFriendIDs()) {
            SparmesUser friend = getUserMasterById(friendId);
            if(!friend.getLogicalPid().equals(t.newPid) && !friend.getLogicalPids().contains(t.newPid)) {
                addLogicalReplica(friendId, t.newPid);
            }
        }

        //Remove replicas as allowed
        //First, remove user replica from new partition if one exists
        if(newPart.getLogicalReplicaIds().contains(t.uid)) {
            removeLogicalReplica(t.uid, t.newPid);
        }

        //TODO: this might not be working properly
        //Second, if we've violated k-constraints, choose another partition at random and replicate this user there
        if(user.getLogicalPids().size() < minNumReplicas) {
            Set<Integer> potentialReplicaLocations = new HashSet<>(pMap.keySet());
            potentialReplicaLocations.remove(user.getLogicalPid());
            potentialReplicaLocations.removeAll(user.getLogicalPids());
            List<Integer> list = new LinkedList<>(potentialReplicaLocations);
            Integer newReplicaPid = list.get((int) (list.size() * Math.random()));
            addLogicalReplica(t.uid, newReplicaPid);
        }

        //Third, remove friends replicas from old partition if they weren't being used for any other reason and don't violate k-replication
        Set<Integer> friendReplicasToRemove = new HashSet<>(user.getFriendIDs());
        friendReplicasToRemove.retainAll(oldPart.getLogicalReplicaIds());
        for(Integer friendId : friendReplicasToRemove) {
            SparmesUser friend = getUserMasterById(friendId);
            Set<Integer> friendsOfFriend = new HashSet<>(friend.getFriendIDs());
            friendsOfFriend.retainAll(oldPart.getLogicalUserIds());
            if(friendsOfFriend.isEmpty() && friend.getLogicalPids().size() > minNumReplicas) {
                oldPart.removeLogicalReplicaId(friendId);
            }
        }
    }

    void updateLogicalUsers() {
        Map<Integer, Integer> pToWeight = getPToWeight(false);
        int totalWeight = 0;
        for(Integer pWeight: pToWeight.values()) {
            totalWeight += pWeight;
        }
        for(SparmesPartition p : pMap.values()) {
            for(Integer logicalUid : p.getLogicalUserIds()) {
                SparmesUser user = getUserMasterById(logicalUid);
                Map<Integer, Integer> updatedFriendCounts = user.getPToFriendCountLogical();
                boolean replicateInSourcePartition = user.shouldReplicateInSourcePartitionLogical();
                int numFriendsToDeleteInCurrentPartition = user.getNumFriendsToDeleteInCurrentPartitionLogical();
                Map<Integer, Integer> friendsToAddInEachPartition = user.getFriendsToAddInEachPartitionLogical();
                LogicalUser luser = new LogicalUser(user.getId(), user.getLogicalPid(), gamma, updatedFriendCounts, pToWeight, user.getLogicalPids(), friendsToAddInEachPartition, numFriendsToDeleteInCurrentPartition, replicateInSourcePartition, totalWeight, minNumReplicas);
                p.addLogicalUser(luser);
            }
        }
    }

    Map<Integer, Integer> getPToWeight(boolean determineWeightsFromPhysicalPartitions) {
        Map<Integer, Integer> pToWeight = new HashMap<>();
        for(Integer partitionId : getAllPartitionIds()) {
            int pWeight;
            if(determineWeightsFromPhysicalPartitions) {
                pWeight = getPartitionById(partitionId).getNumMasters();
            }
            else {
                pWeight = getPartitionById(partitionId).getNumLogicalUsers();
            }
            pToWeight.put(partitionId, pWeight);
        }
        return pToWeight;
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
            if(!observedMasterPid.equals(getUserMasterById(uid).getMasterPid())) {
                throw new RuntimeException("Mismatch between user's pid and partition's");
            }

            //TODO: should we check the logical partitions?
        }

        //check replicas
        for(Integer uid : uMap.keySet()) {
            SparmesUser user = getUserMasterById(uid);
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
