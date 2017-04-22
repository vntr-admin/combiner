package io.vntr.replicadummy;

import io.vntr.IRepManager;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class ReplicaDummyManager implements IRepManager {

    private int minNumReplicas;

    private static final Integer defaultStartingId = 1;

    private SortedMap<Integer, ReplicaDummyPartition> partitionIdToPartitionMap;

    private NavigableMap<Integer, Integer> userIdToMasterPartitionIdMap = new TreeMap<>();

    public ReplicaDummyManager(int minNumReplicas) {
        this.minNumReplicas = minNumReplicas;
        partitionIdToPartitionMap = new TreeMap<>();
    }

    @Override
    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public ReplicaDummyPartition getPartitionById(Integer id) {
        return partitionIdToPartitionMap.get(id);
    }

    @Override
    public RepUser getUserMasterById(Integer id) {
        Integer partitionId = userIdToMasterPartitionIdMap.get(id);
        if (partitionId != null) {
            ReplicaDummyPartition partition = getPartitionById(partitionId);
            if (partition != null) {
                return partition.getMasterById(id);
            }
        }
        return null;
    }

    @Override
    public int getNumUsers() {
        return userIdToMasterPartitionIdMap.size();
    }

    @Override
    public Set<Integer> getPids() {
        return partitionIdToPartitionMap.keySet();
    }

    @Override
    public Set<Integer> getUids() {
        return userIdToMasterPartitionIdMap.keySet();
    }

    @Override
    public int addUser() {
        int newUid = userIdToMasterPartitionIdMap.lastKey() + 1;
        addUser(new User(newUid));
        return newUid;
    }

    @Override
    public void addUser(User user) {
        Integer masterPartitionId = getPartitionIdWithFewestMasters();

        RepUser RepUser = new RepUser(user.getId());
        RepUser.setBasePid(masterPartitionId);

        addUser(RepUser, masterPartitionId);

        for (Integer id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(RepUser, id);
        }
    }

    @Override
    public void addUser(RepUser user, Integer masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        userIdToMasterPartitionIdMap.put(user.getId(), masterPartitionId);
    }

    @Override
    public void removeUser(Integer userId) {
        RepUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getBasePid()).removeMaster(userId);
        for (Integer replicaPartitionId : user.getReplicaPids()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from userIdToMasterPartitionIdMap
        userIdToMasterPartitionIdMap.remove(userId);

        //Remove friendships
        for (Integer friendId : user.getFriendIDs()) {
            RepUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Integer friendReplicaPartitionId : friendMaster.getReplicaPids()) {
                ReplicaDummyPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(userId);
            }
        }
    }

    @Override
    public Integer addPartition() {
        Integer newId = partitionIdToPartitionMap.isEmpty() ? defaultStartingId : partitionIdToPartitionMap.lastKey() + 1;
        addPartition(newId);
        return newId;
    }

    @Override
    public void addPartition(Integer pid) {
        partitionIdToPartitionMap.put(pid, new ReplicaDummyPartition(pid));
    }

    @Override
    public void removePartition(Integer id) {
        partitionIdToPartitionMap.remove(id);
    }

    @Override
    public void addReplica(RepUser user, Integer destPid) {
        RepUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Integer pid : user.getReplicaPids()) {
            partitionIdToPartitionMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    @Override
    public RepUser addReplicaNoUpdates(RepUser user, Integer destPid) {
        RepUser replica = user.dupe();
        partitionIdToPartitionMap.get(destPid).addReplica(replica);
        return replica;
    }

    @Override
    public void removeReplica(RepUser user, Integer removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
            ReplicaDummyPartition p = partitionIdToPartitionMap.get(currentReplicaPartitionId);
            RepUser r = p.getReplicaById(user.getId());
            r.removeReplicaPartitionId(removalPartitionId);
        }

        //Delete it from the master's replicaPartitionIds
        user.removeReplicaPartitionId(removalPartitionId);

        //Actually remove the replica from the partition itself
        partitionIdToPartitionMap.get(removalPartitionId).removeReplica(user.getId());
    }

    @Override
    public void moveUser(RepUser user, Integer toPid, Set<Integer> replicateInDestinationPartition, Set<Integer> replicasToDeleteInSourcePartition) {
        //Step 1: move the actual user
        Integer uid = user.getId();
        Integer fromPid = user.getBasePid();

        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for (Integer friendId : user.getFriendIDs()) {
            if (userIdToMasterPartitionIdMap.get(friendId).equals(fromPid)) {
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

    @Override
    public void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid) {
        RepUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        userIdToMasterPartitionIdMap.put(uid, toPid);

        user.setBasePid(toPid);

        for (Integer rPid : user.getReplicaPids()) {
            partitionIdToPartitionMap.get(rPid).getReplicaById(uid).setBasePid(toPid);
        }
    }

    @Override
    public void befriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.befriend(largerUser.getId());
        largerUser.befriend(smallerUser.getId());

        for (Integer replicaPartitionId : smallerUser.getReplicaPids()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Integer replicaPartitionId : largerUser.getReplicaPids()) {
            partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    @Override
    public void unfriend(RepUser smallerUser, RepUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Integer partitionId : smallerUser.getReplicaPids()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Integer partitionId : largerUser.getReplicaPids()) {
            partitionIdToPartitionMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    @Override
    public void promoteReplicaToMaster(Integer userId, Integer partitionId) {
        ReplicaDummyPartition partition = partitionIdToPartitionMap.get(partitionId);
        RepUser user = partition.getReplicaById(userId);
        user.setBasePid(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        userIdToMasterPartitionIdMap.put(userId, partitionId);

        for (Integer replicaPartitionId : user.getReplicaPids()) {
            RepUser replica = partitionIdToPartitionMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setBasePid(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }

        //Add replicas of friends in pid if they don't already exist
        for(int friendId : user.getFriendIDs()) {
            if(!partition.getIdsOfMasters().contains(friendId) && !partition.getIdsOfReplicas().contains(friendId)) {
                addReplica(getUserMasterById(friendId), partitionId);
            }
        }
    }

    @Override
    public Integer getPartitionIdWithFewestMasters() {
        int minMasters = Integer.MAX_VALUE;
        Integer minId = -1;

        for (Integer id : partitionIdToPartitionMap.keySet()) {
            int numMasters = getPartitionById(id).getNumMasters();
            if (numMasters < minMasters) {
                minMasters = numMasters;
                minId = id;
            }
        }

        return minId;
    }

    @Override
    public Integer getRandomPartitionIdWhereThisUserIsNotPresent(RepUser user) {
        Set<Integer> potentialReplicaLocations = new HashSet<>(partitionIdToPartitionMap.keySet());
        potentialReplicaLocations.remove(user.getBasePid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        List<Integer> list = new LinkedList<>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    @Override
    public Set<Integer> getPartitionsToAddInitialReplicas(Integer masterPartitionId) {
        List<Integer> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<>(partitionIdToPartitionMap.keySet());
        partitionIdsAtWhichReplicasCanBeAdded.remove(masterPartitionId);
        return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : partitionIdToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToReplicasMap() {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        for (Integer pid : partitionIdToPartitionMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    @Override
    public Integer getEdgeCut() {
        int count = 0;
        for (Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            RepUser user = getUserMasterById(uid);
            Integer pid = user.getBasePid();
            for (Integer friendId : user.getFriendIDs()) {
                if (!pid.equals(userIdToMasterPartitionIdMap.get(friendId))) {
                    count++;
                }
            }
        }
        return count / 2;
    }

    @Override
    public Integer getReplicationCount() {
        int count = 0;
        for(Integer pid : getPids()) {
            count += getPartitionById(pid).getNumReplicas();
        }
        return count;
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        for(Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            friendships.put(uid, getUserMasterById(uid).getFriendIDs());
        }
        return friendships;
    }

    @Override
    public String toString() {
        return "minNumReplicas:" + minNumReplicas + "|#U:" + getNumUsers() + "|#P:" + partitionIdToPartitionMap.size();
    }

    @Override
    public void checkValidity() {
        boolean valid = true;
        for(Integer uid : userIdToMasterPartitionIdMap.keySet()) {
            RepUser user = getUserMasterById(uid);
            Integer observedMasterPid = null;
            Set<Integer> replicaPidsFromPartitions = new HashSet<>();
            for(Integer pid : partitionIdToPartitionMap.keySet()) {
                ReplicaDummyPartition p = partitionIdToPartitionMap.get(pid);
                if(p.getIdsOfReplicas().contains(uid)) {
                    replicaPidsFromPartitions.add(pid);
                }
                if(p.getIdsOfMasters().contains(observedMasterPid)) {
                    if(observedMasterPid != null) {
                        throw new RuntimeException("user cannot have master in multiple partitions");
                    }
                    observedMasterPid = pid;
                }
            }

            if(observedMasterPid == null) {
                throw new RuntimeException("user must have a master in some partition");
            }
            if(!observedMasterPid.equals(user.getBasePid())) {
                throw new RuntimeException("Mismatch between user's master PID and system's");
            }
            if(replicaPidsFromPartitions.size() < minNumReplicas) {
                throw new RuntimeException("Insufficient replicas");
            }
            if(!replicaPidsFromPartitions.equals(user.getReplicaPids())) {
                throw new RuntimeException("Mismatch between user's replica PIDs and system's");
            }
        }
    }

    @Override
    public void increaseTally(int amount) {
        //replica dummy does not migrate
    }

    @Override
    public void increaseTallyLogical(int amount) {
        //replica dummy does not migrate
    }

    @Override
    public long getMigrationTally() {
        return 0; //replica dummy does not migrate
    }

}
