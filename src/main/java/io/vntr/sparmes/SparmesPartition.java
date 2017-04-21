package io.vntr.sparmes;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesPartition {
    private final float gamma;
    private Map<Integer, SparmesUser> idToMasterMap = new HashMap<>();
    private Map<Integer, SparmesUser> idToReplicaMap = new HashMap<>();
    private Map<Integer, LogicalUser> logicalUsers = new HashMap<>();
    private Set<Integer> logicalReplicaIds = new HashSet<>();
    private Integer id;
    private SparmesManager manager;

    public SparmesPartition(Integer id, float gamma, SparmesManager manager) {
        this.id = id;
        this.manager = manager;
        this.gamma = gamma;
    }

    public void addMaster(SparmesUser user) {
        idToMasterMap.put(user.getId(), user);
    }

    public User removeMaster(Integer id) {
        return idToMasterMap.remove(id);
    }

    public void addReplica(SparmesUser user) {
        idToReplicaMap.put(user.getId(), user);
    }

    public User removeReplica(Integer id) {
        return idToReplicaMap.remove(id);
    }

    public SparmesUser getMasterById(Integer userId) {
        return idToMasterMap.get(userId);
    }

    public SparmesUser getReplicaById(Integer userId) {
        return idToReplicaMap.get(userId);
    }

    public int getNumMasters() {
        return idToMasterMap.size();
    }

    public int getNumReplicas() {
        return idToReplicaMap.size();
    }

    public Set<Integer> getIdsOfMasters() {
        return idToMasterMap.keySet();
    }

    public Set<Integer> getIdsOfReplicas() {
        return idToReplicaMap.keySet();
    }

    public Integer getId() {
        return id;
    }

    public Set<Integer> getLogicalReplicaIds() {
        return logicalReplicaIds;
    }

    public void removeLogicalReplicaId(Integer uid) {
        logicalReplicaIds.remove(uid);
    }

    public void addLogicalReplicaId(Integer uid) {
        logicalReplicaIds.add(uid);
    }

    public void resetLogicalUsers() {
        logicalUsers = new HashMap<>();
        for(SparmesUser user : idToMasterMap.values()) {
            logicalUsers.put(user.getId(), user.getLogicalUser(true));
        }
        logicalReplicaIds.clear();
        logicalReplicaIds.addAll(idToReplicaMap.keySet());
    }

    public Set<Target> getCandidates(boolean firstIteration, int k, boolean probabilistic) {
        NavigableSet<Target> candidates = new TreeSet<>();
        for(LogicalUser luser : logicalUsers.values()) {
            Target target = luser.getTargetPart(firstIteration, probabilistic);
            if(target.newPid != null) {
                candidates.add(target);
            }
        }

        Set<Target> topKCandidates = new HashSet<>();
        int i=0;
        for(Iterator<Target> iter = candidates.descendingIterator(); iter.hasNext() && i++<k; ) {
            topKCandidates.add(iter.next());
        }

        return topKCandidates;
    }

    public Set<Integer> physicallyCopyNewMasters() {
        Set<Integer> newMasters = new HashSet<>(logicalUsers.keySet());
        newMasters.removeAll(idToMasterMap.keySet());
        for(Integer newUid : newMasters) {
            SparmesUser user = manager.getUserMasterById(newUid);
            user.setLogicalPid(id);
            user.setMasterPid(id);
            for (Integer rPid : user.getReplicaPids()) {
                manager.getPartitionById(rPid).getReplicaById(newUid).setMasterPid(id);
            }
            idToMasterMap.put(newUid, user);
        }
        return newMasters;
    }

    public void physicallyCopyNewReplicas() {
        Set<Integer> toReplicate = new HashSet<>(logicalReplicaIds);
        toReplicate.removeAll(idToReplicaMap.keySet());
        for(Integer uid : toReplicate) {
            manager.addReplica(manager.getUserMasterById(uid), id);
        }
    }

    public void physicallyMigrateDelete() {
        physicallyDeleteOldMasters();
        physicallyDeleteOldReplicas();
    }

    void physicallyDeleteOldMasters() {
        idToMasterMap.keySet().retainAll(logicalUsers.keySet());
    }

    void physicallyDeleteOldReplicas() {
        Set<Integer> toDereplicate = new HashSet<>(idToReplicaMap.keySet());
        toDereplicate.removeAll(logicalReplicaIds);
        for(Integer uid : toDereplicate) {
            manager.removeReplica(manager.getUserMasterById(uid), id);
        }
    }

    public int getNumLogicalUsers() {
        return logicalUsers.size();
    }

    public Set<Integer> getLogicalUserIds() {
        return logicalUsers.keySet();
    }

    public void removeLogicalUser(Integer userId) {
        logicalUsers.remove(userId);
    }

    public void addLogicalUser(LogicalUser logicalUser) {
        this.logicalUsers.put(logicalUser.getId(), logicalUser);
    }

    public void updateLogicalUsersPartitionWeights(Map<Integer, Integer> pToWeight) {
        for(LogicalUser logicalUser : logicalUsers.values()) {
            logicalUser.setpToWeight(pToWeight);
        }
    }

    public void updateLogicalUsersTotalWeights(Integer totalWeight) {
        for(LogicalUser logicalUser : logicalUsers.values()) {
            logicalUser.setTotalWeight(totalWeight);
        }
    }

    public void updateLogicalUserFriendCounts(Integer logicalUid, Map<Integer, Integer> updatedFriendCounts) {
        logicalUsers.get(logicalUid).setPToFriendCount(updatedFriendCounts);
    }

    public void shoreUpFriendReplicas() {
        for(SparmesUser user : idToMasterMap.values()) {
            Set<Integer> friends = new HashSet<>(user.getFriendIDs());
            friends.removeAll(idToMasterMap.keySet());
            friends.removeAll(idToReplicaMap.keySet());
            for(int friendId : friends) {
                manager.addReplica(manager.getUserMasterById(friendId), id);
            }
        }
    }
}
