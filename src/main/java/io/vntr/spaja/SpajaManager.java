package io.vntr.spaja;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SpajaManager {
    private int minNumReplicas;
    private int randomSamplingSize;
    private float alpha;
    private float initialT;
    private float deltaT;

    private int nextPid = 1;
    private int nextUid = 1;
    private long migrationTally;

    private Map<Integer, SpajaPartition> pMap;

    private Map<Integer, Integer> uMap = new HashMap<>();

    public SpajaManager(int minNumReplicas, float alpha, float initialT, float deltaT, int randomSamplingSize) {
        this.minNumReplicas = minNumReplicas;
        this.pMap = new HashMap<>();
        this.alpha = alpha;
        this.initialT = initialT;
        this.deltaT = deltaT;
        this.randomSamplingSize = randomSamplingSize;
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public float getAlpha() {
        return alpha;
    }

    public float getInitialT() {
        return initialT;
    }

    public float getDeltaT() {
        return deltaT;
    }

    public int getRandomSamplingSize() {
        return randomSamplingSize;
    }

    public SpajaPartition getPartitionById(Integer id) {
        return pMap.get(id);
    }

    public Integer getMasterPartitionIdForUser(Integer uid) {
        return uMap.get(uid);
    }

    public SpajaUser getUserMasterById(Integer id) {
        Integer pid = uMap.get(id);
        return getPartitionById(pid).getMasterById(id);
    }

    public Set<SpajaUser> getUserMastersById(Collection<Integer> ids) {
        Set<SpajaUser> users = new HashSet<>();
        for(Integer uid : ids) {
            users.add(getUserMasterById(uid));
        }
        return users;
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

    public void addUser(User user) {
        Integer masterPartitionId = getPartitionIdWithFewestMasters();

        int uid = user.getId();
        SpajaUser spajaUser = new SpajaUser(uid, alpha, minNumReplicas, this);
        spajaUser.setMasterPid(masterPartitionId);

        addUser(spajaUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(spajaUser, id);
        }
    }

    void addUser(SpajaUser user, Integer masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        uMap.put(user.getId(), masterPartitionId);
        if(user.getId() >= nextUid) {
            nextUid = user.getId() + 1;
        }

    }

    public void removeUser(Integer userId) {
        SpajaUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getMasterPid()).removeMaster(userId);
        for (Integer replicaPartitionId : user.getReplicaPids()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from uMap
        uMap.remove(userId);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            SpajaUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Integer friendReplicaPartitionId : friendMaster.getReplicaPids()) {
                SpajaPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
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
        pMap.put(pid, new SpajaPartition(pid));
        if(pid >= nextPid) {
            nextPid = pid + 1;
        }
    }

    public void removePartition(Integer id) {
        pMap.remove(id);
    }

    public void addReplica(SpajaUser user, Integer destPid) {
        SpajaUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Integer pid : user.getReplicaPids()) {
            pMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    SpajaUser addReplicaNoUpdates(SpajaUser user, Integer destPid) {
        SpajaUser replica = user.clone();
        pMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(SpajaUser user, Integer removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
            pMap.get(currentReplicaPartitionId).getReplicaById(user.getId()).removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        pMap.get(removalPartitionId).removeReplica(user.getId());
    }

    public void befriend(SpajaUser smallerUser, SpajaUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPids()) {
            pMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPids()) {
            pMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(SpajaUser smallerUser, SpajaUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPids()) {
            pMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPids()) {
            pMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void moveUser(SpajaUser user, Integer toPid, Set<Integer> replicateInDestPartition, Set<Integer> replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        Integer uid = user.getId();
        Integer fromPid = user.getMasterPid();

        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for (Integer friendId : user.getFriendIDs()) {
            if (uMap.get(friendId).equals(fromPid)) {
                addReplica(user, fromPid);
                break;
            }
        }

        for (Integer friendToReplicateId : replicateInDestPartition) {
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

        increaseMigrationTally(1);
    }

    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        SpajaPartition partition = pMap.get(partitionId);
        SpajaUser user = partition.getReplicaById(userId);
        user.setMasterPid(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        uMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPids()) {
            SpajaUser replica = pMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setMasterPid(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }

        //Add replicas of friends in pid if they don't already exist
        for(int friendId : user.getFriendIDs()) {
            if(!partition.getIdsOfMasters().contains(friendId) && !partition.getIdsOfReplicas().contains(friendId)) {
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

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(SpajaUser user) {
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
            SpajaUser user = getUserMasterById(uid);
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
        SpajaUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        uMap.put(uid, toPid);

        user.setMasterPid(toPid);

        for (Integer rPid : user.getReplicaPids()) {
            pMap.get(rPid).getReplicaById(uid).setMasterPid(toPid);
        }
    }

    public void swap(Integer uid1, Integer uid2, SpajaBefriendingStrategy strategy) {
        SpajaUser u1 = getUserMasterById(uid1);
        SpajaUser u2 = getUserMasterById(uid2);

        Integer pid1 = u1.getMasterPid();
        Integer pid2 = u2.getMasterPid();

        //TODO: make sure this adds friend replicas on the new partition
        SwapChanges swapChanges = strategy.getSwapChanges(u1, u2);

        moveMasterAndInformReplicas(uid1, pid1, pid2);
        moveMasterAndInformReplicas(uid2, pid2, pid1);


        for(Integer uid : swapChanges.getAddToP1()) {
            addReplica(getUserMasterById(uid), pid1);
        }

        for(Integer uid : swapChanges.getAddToP2()) {
            addReplica(getUserMasterById(uid), pid2);
        }

        for(Integer uid : swapChanges.getRemoveFromP1()) {
            removeReplica(getUserMasterById(uid), pid1);
        }

        for(Integer uid : swapChanges.getRemoveFromP2()) {
            removeReplica(getUserMasterById(uid), pid2);
        }

        increaseMigrationTally(2);

//        if(!isInAValidState()) {
//            isInAValidState();
//        }
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : uMap.keySet()) {
            friendships.put(uid, getUserMasterById(uid).getFriendIDs());
        }
        return friendships;
    }


    public int addUser() {
        int newUid = nextUid;
        addUser(new User(newUid));
        return newUid;
    }

    public static Set<Integer> findKeysForUser(Map<Integer, Set<Integer>> m, int uid) {
        Set<Integer> keys = new HashSet<>();
        for(int key : m.keySet()) {
            if(m.get(key).contains(uid)) {
                keys.add(key);
            }
        }
        return keys;
    }

    public Map<Integer, Set<Integer>> getPartitionToReplicaMap() {
        Map<Integer, Set<Integer>> m = new HashMap<>();
        for(int pid : pMap.keySet()) {
            m.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return m;
    }

    public long getMigrationTally() {
        return migrationTally;
    }

    void increaseMigrationTally(int amount) {
        migrationTally += amount;
    }

    @Override
    public String toString() {
        return "minNumReplicas:" + minNumReplicas + "|k:" + randomSamplingSize + "|alpha:" + alpha + "|initialT:" + initialT + "|deltaT:" + deltaT + "|#U:" + getNumUsers() + "|#P:" + pMap.size();
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
        }

        //check replicas
        for(Integer uid : uMap.keySet()) {
            SpajaUser user = getUserMasterById(uid);
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

}
