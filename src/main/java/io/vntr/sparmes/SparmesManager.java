package io.vntr.sparmes;

import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesManager {
    private int minNumReplicas;

    private static final Long defaultStartingId = 1L;

    private SortedMap<Long, SparmesPartition> pMap;

    private Map<Long, Long> uMap = new HashMap<Long, Long>();

    public SparmesManager(int minNumReplicas) {
        this.minNumReplicas = minNumReplicas;
        this.pMap = new TreeMap<Long, SparmesPartition>();
    }

    public int getMinNumReplicas() {
        return minNumReplicas;
    }

    public SparmesPartition getPartitionById(Long id) {
        return pMap.get(id);
    }

    public SparmesUser getUserMasterById(Long id) {
        Long partitionId = uMap.get(id);
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

    public Set<Long> getAllPartitionIds() {
        return pMap.keySet();
    }

    public Set<Long> getAllUserIds() {
        return uMap.keySet();
    }

    public void addUser(User user) {
        Long masterPartitionId = getPartitionIdWithFewestMasters();

        SparmesUser spajaUser = new SparmesUser(user.getName(), user.getId());
        spajaUser.setMasterPartitionId(masterPartitionId);
        spajaUser.setPartitionId(masterPartitionId);

        addUser(spajaUser, masterPartitionId);

        for (Long id : getPartitionsToAddInitialReplicas(masterPartitionId)) {
            addReplica(spajaUser, id);
        }
    }

    void addUser(SparmesUser user, Long masterPartitionId) {
        getPartitionById(masterPartitionId).addMaster(user);
        uMap.put(user.getId(), masterPartitionId);
    }

    public void removeUser(Long userId) {
        SparmesUser user = getUserMasterById(userId);

        //Remove user from relevant partitions
        getPartitionById(user.getMasterPartitionId()).removeMaster(userId);
        for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
            getPartitionById(replicaPartitionId).removeReplica(userId);
        }

        //Remove user from uMap
        uMap.remove(userId);

        //Remove friendships
        for (Long friendId : user.getFriendIDs()) {
            SparmesUser friendMaster = getUserMasterById(friendId);
            friendMaster.unfriend(userId);

            for (Long friendReplicaPartitionId : friendMaster.getReplicaPartitionIds()) {
                SparmesPartition friendReplicaPartition = getPartitionById(friendReplicaPartitionId);
                friendReplicaPartition.getReplicaById(friendId).unfriend(userId);
            }
        }
    }

    public Long addPartition() {
        Long newId = pMap.isEmpty() ? defaultStartingId : pMap.lastKey() + 1;
        addPartition(newId);
        return newId;
    }

    void addPartition(Long pid) {
        pMap.put(pid, new SparmesPartition(pid));
    }

    public void removePartition(Long id) {
        pMap.remove(id);
    }

    public void addReplica(SparmesUser user, Long destPid) {
        SparmesUser replicaOfUser = addReplicaNoUpdates(user, destPid);

        //Update the replicaPartitionIds to reflect this addition
        replicaOfUser.addReplicaPartitionId(destPid);
        for (Long pid : user.getReplicaPartitionIds()) {
            pMap.get(pid).getReplicaById(user.getId()).addReplicaPartitionId(destPid);
        }
        user.addReplicaPartitionId(destPid);
    }

    SparmesUser addReplicaNoUpdates(SparmesUser user, Long destPid) {
        SparmesUser replica = user.clone();
        replica.setPartitionId(destPid);
        pMap.get(destPid).addReplica(replica);
        return replica;
    }

    public void removeReplica(SparmesUser user, Long removalPartitionId) {
        //Delete it from each replica's replicaPartitionIds
        for (Long currentReplicaPartitionId : user.getReplicaPartitionIds()) {
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

        for (Long replicaPartitionId : smallerUser.getReplicaPartitionIds()) {
            pMap.get(replicaPartitionId).getReplicaById(smallerUser.getId()).befriend(largerUser.getId());
        }

        for (Long replicaPartitionId : largerUser.getReplicaPartitionIds()) {
            pMap.get(replicaPartitionId).getReplicaById(largerUser.getId()).befriend(smallerUser.getId());
        }
    }

    public void unfriend(SparmesUser smallerUser, SparmesUser largerUser) {
        smallerUser.unfriend(largerUser.getId());
        largerUser.unfriend(smallerUser.getId());

        for (Long partitionId : smallerUser.getReplicaPartitionIds()) {
            pMap.get(partitionId).getReplicaById(smallerUser.getId()).unfriend(largerUser.getId());
        }

        for (Long partitionId : largerUser.getReplicaPartitionIds()) {
            pMap.get(partitionId).getReplicaById(largerUser.getId()).unfriend(smallerUser.getId());
        }
    }

    public void moveUser(SparmesUser user, Long toPid, Set<Long> replicateInDestinationPartition, Set<Long> replicasToDeleteInSourcePartition) {
        Long uid = user.getId();
        Long fromPid = user.getMasterPartitionId();

        //Step 1: move the actual user
        moveMasterAndInformReplicas(uid, fromPid, toPid);

        //Step 2: add the necessary replicas
        for (Long friendId : user.getFriendIDs()) {
            if (uMap.get(friendId).equals(fromPid)) {
                addReplica(user, fromPid);
                break;
            }
        }

        for (Long friendToReplicateId : replicateInDestinationPartition) {
            addReplica(getUserMasterById(friendToReplicateId), toPid);
        }

        //Step 3: remove unnecessary replicas
        //Possibilities:
        // (1) replica of user in destinationPartition
        // (2) replicas of user's friends in oldPartition with no other purpose
        // (3) [the replica of the new friend that prompted this move should already be accounted for in (2)]

        if (user.getReplicaPartitionIds().contains(toPid)) {
            if (user.getReplicaPartitionIds().size() <= minNumReplicas) {
                //add one in another partition that doesn't yet have one of this user
                addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user));
            }
            removeReplica(user, toPid);
        }

        //delete the replica of the appropriate friends in oldPartition
        for (Long replicaIdToDelete : replicasToDeleteInSourcePartition) {
            removeReplica(getUserMasterById(replicaIdToDelete), fromPid);
        }
    }

    public void promoteReplicaToMaster(Long userId, Long partitionId) {
        SparmesPartition partition = pMap.get(partitionId);
        SparmesUser user = partition.getReplicaById(userId);
        user.setMasterPartitionId(partitionId);
        user.removeReplicaPartitionId(partitionId);
        partition.addMaster(user);
        partition.removeReplica(userId);

        uMap.put(userId, partitionId);

        for (Long replicaPartitionId : user.getReplicaPartitionIds()) {
            SparmesUser replica = pMap.get(replicaPartitionId).getReplicaById(userId);
            replica.setMasterPartitionId(partitionId);
            replica.removeReplicaPartitionId(partitionId);
        }
    }

    Long getPartitionIdWithFewestMasters() {
        int minMasters = Integer.MAX_VALUE;
        Long minId = -1L;

        for (Long id : pMap.keySet()) {
            int numMasters = getPartitionById(id).getNumMasters();
            if (numMasters < minMasters) {
                minMasters = numMasters;
                minId = id;
            }
        }

        return minId;
    }

    Long getRandomPartitionIdWhereThisUserIsNotPresent(SparmesUser user) {
        Set<Long> potentialReplicaLocations = new HashSet<Long>(pMap.keySet());
        potentialReplicaLocations.remove(user.getMasterPartitionId());
        potentialReplicaLocations.removeAll(user.getReplicaPartitionIds());
        List<Long> list = new LinkedList<Long>(potentialReplicaLocations);
        return list.get((int) (list.size() * Math.random()));
    }

    Set<Long> getPartitionsToAddInitialReplicas(Long masterPartitionId) {
        List<Long> partitionIdsAtWhichReplicasCanBeAdded = new LinkedList<Long>(pMap.keySet());
        partitionIdsAtWhichReplicasCanBeAdded.remove(masterPartitionId);
        return ProbabilityUtils.getKDistinctValuesFromList(getMinNumReplicas(), partitionIdsAtWhichReplicasCanBeAdded);
    }

    public Map<Long, Set<Long>> getPartitionToUserMap() {
        Map<Long, Set<Long>> map = new HashMap<Long, Set<Long>>();
        for (Long pid : pMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfMasters());
        }
        return map;
    }

    Map<Long, Set<Long>> getPartitionToReplicasMap() {
        Map<Long, Set<Long>> map = new HashMap<Long, Set<Long>>();
        for (Long pid : pMap.keySet()) {
            map.put(pid, getPartitionById(pid).getIdsOfReplicas());
        }
        return map;
    }

    public Long getEdgeCut() {
        long count = 0L;
        for (Long uid : uMap.keySet()) {
            SparmesUser user = getUserMasterById(uid);
            Long pid = user.getMasterPartitionId();
            for (Long friendId : user.getFriendIDs()) {
                if (!pid.equals(uMap.get(friendId))) {
                    count++;
                }
            }
        }
        return count / 2;
    }

    public Long getReplicationCount() {
        int count = 0;
        for(Long pid : getAllPartitionIds()) {
            count += getPartitionById(pid).getNumReplicas();
        }
        return (long) count;
    }

    void moveMasterAndInformReplicas(Long uid, Long fromPid, Long toPid) {
        SparmesUser user = getUserMasterById(uid);
        getPartitionById(fromPid).removeMaster(uid);
        getPartitionById(toPid).addMaster(user);

        uMap.put(uid, toPid);

        user.setMasterPartitionId(toPid);
        user.setPartitionId(toPid);

        for (Long rPid : user.getReplicaPartitionIds()) {
            pMap.get(rPid).getReplicaById(uid).setMasterPartitionId(toPid);
        }
    }

    public void repartition() {
        int k = 3; //TODO: set this intelligently
        for (SparmesPartition p : pMap.values()) {
            p.resetLogicalUsers();
        }

        boolean stoppingCondition = false;
        while (!stoppingCondition) {
            boolean changed = false;

            Map<Long, Set<Target>> firstStageTargets = new HashMap<Long, Set<Target>>();
            for (SparmesPartition p : pMap.values()) {
                Set<Target> targets = p.getCandidates(true, k);
                firstStageTargets.put(p.getId(), targets);
                changed |= !targets.isEmpty();
            }

            for(Long pid : pMap.keySet()) {
                for(Target target : firstStageTargets.get(pid)) {
                    migrateLogically(target);
                }
            }

            updateAggregateWeightInformation();

            Map<Long, Set<Target>> secondStageTargets = new HashMap<Long, Set<Target>>();
            for (SparmesPartition p : pMap.values()) {
                Set<Target> targets = p.getCandidates(false, k);
                secondStageTargets.put(p.getId(), targets);
                changed |= !targets.isEmpty();
            }

            for(Long pid : pMap.keySet()) {
                for(Target target : secondStageTargets .get(pid)) {
                    migrateLogically(target);
                }
            }

            updateAggregateWeightInformation();

            stoppingCondition = !changed;
        }

        Map<Long, Long> usersWhoMoved = new HashMap<Long, Long>();
        for (SparmesPartition p : pMap.values()) {
            Set<Long> moved = p.physicallyMigrateCopy();
            for(Long uid : moved) {
                usersWhoMoved.put(uid, p.getId());
            }
        }

        for (SparmesPartition p : pMap.values()) {
            p.physicallyMigrateDelete();
        }

        uMap.putAll(usersWhoMoved);
    }

    void migrateLogically(Target target) {
        //TODO: do this
    }

    void updateAggregateWeightInformation() {
        //TODO: do this
    }
}
