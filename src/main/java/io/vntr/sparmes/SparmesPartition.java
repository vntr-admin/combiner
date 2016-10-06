package io.vntr.sparmes;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesPartition {
    private final float gamma;
    private Map<Integer, SparmesUser> idToMasterMap = new HashMap<Integer, SparmesUser>();
    private Map<Integer, SparmesUser> idToReplicaMap = new HashMap<Integer, SparmesUser>();
    private Map<Integer, LogicalUser> logicalUsers = new HashMap<Integer, LogicalUser>();
    private Set<Integer> logicalReplicaIds = new HashSet<Integer>();
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
        logicalUsers = new HashMap<Integer, LogicalUser>();
        for(SparmesUser user : idToMasterMap.values()) {
            logicalUsers.put(user.getId(), user.getLogicalUser(true));
        }
    }

    public Set<Target> getCandidates(boolean firstIteration, int k, boolean probabilistic) {
        NavigableSet<Target> candidates = new TreeSet<Target>();
        for(LogicalUser luser : logicalUsers.values()) {
            Target target = luser.getTargetPart(firstIteration, probabilistic);
            if(target.newPid != null) {
                candidates.add(target);
            }
        }

        Set<Target> topKCandidates = new HashSet<Target>();
        int i=0;
        for(Iterator<Target> iter = candidates.descendingIterator(); iter.hasNext() && i++<k; ) {
            topKCandidates.add(iter.next());
        }

        return topKCandidates;
    }


    public Set<Integer> physicallyMigrateCopy() {
        Set<Integer> logicalUserSet = new HashSet<Integer>(logicalUsers.keySet());
        Set<Integer> friendsSet = new HashSet<Integer>();
        logicalUserSet.removeAll(idToMasterMap.keySet());
        for(Integer newUid : logicalUserSet) {
            SparmesUser user = manager.getUserMasterById(newUid);
            idToMasterMap.put(newUid, user);
            friendsSet.addAll(user.getFriendIDs());
        }

        Set<Integer> toReplicate = new HashSet<Integer>(logicalReplicaIds);
        toReplicate.removeAll(idToReplicaMap.keySet());
        for(Integer uid : toReplicate) {
            manager.addReplica(manager.getUserMasterById(uid), id);
        }

        return logicalUserSet;
    }

    public void physicallyMigrateDelete() {
        Set<Integer> removedUsers = new HashSet<Integer>();
        for(Iterator<Integer> iter = idToMasterMap.keySet().iterator(); iter.hasNext(); ) {
            Integer uid = iter.next();
            SparmesUser user = idToMasterMap.get(uid);
            Integer logicalPid = user.getLogicalPid();
            if(!logicalPid.equals(id)) {
                user.setMasterPartitionId(logicalPid);
                removedUsers.add(uid);
                iter.remove();
            }
        }

        Set<Integer> toDereplicate = new HashSet<Integer>(idToReplicaMap.keySet());
        toDereplicate.removeAll(logicalReplicaIds);
        for(Integer uid : toDereplicate) {
            SparmesUser user = manager.getUserMasterById(uid);
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

}
