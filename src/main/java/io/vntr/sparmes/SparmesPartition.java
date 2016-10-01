package io.vntr.sparmes;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesPartition {
    private final double gamma;
    private Map<Long, SparmesUser> idToMasterMap = new HashMap<Long, SparmesUser>();
    private Map<Long, SparmesUser> idToReplicaMap = new HashMap<Long, SparmesUser>();
    private Map<Long, LogicalUser> logicalUsers = new HashMap<Long, LogicalUser>();
    private Long id;
    private SparmesManager manager;

    public SparmesPartition(Long id, double gamma, SparmesManager manager) {
        this.id = id;
        this.manager = manager;
        this.gamma = gamma;
    }

    public void addMaster(SparmesUser user) {
        idToMasterMap.put(user.getId(), user);
    }

    public User removeMaster(Long id) {
        return idToMasterMap.remove(id);
    }

    public void addReplica(SparmesUser user) {
        idToReplicaMap.put(user.getId(), user);
    }

    public User removeReplica(Long id) {
        return idToReplicaMap.remove(id);
    }

    public SparmesUser getMasterById(Long userId) {
        return idToMasterMap.get(userId);
    }

    public SparmesUser getReplicaById(Long userId) {
        return idToReplicaMap.get(userId);
    }

    public int getNumMasters() {
        return idToMasterMap.size();
    }

    public int getNumReplicas() {
        return idToReplicaMap.size();
    }

    public Set<Long> getIdsOfMasters() {
        return idToMasterMap.keySet();
    }

    public Set<Long> getIdsOfReplicas() {
        return idToReplicaMap.keySet();
    }

    public Long getId() {
        return id;
    }

    public void resetLogicalUsers() {
        logicalUsers = new HashMap<Long, LogicalUser>();
        for(SparmesUser user : idToMasterMap.values()) {
            logicalUsers.put(user.getId(), user.getLogicalUser(true));
        }
    }

    public Set<Target> getCandidates(boolean firstIteration, int k) {
        NavigableSet<Target> candidates = new TreeSet<Target>();
        for(LogicalUser luser : logicalUsers.values()) {
            Target target = luser.getTargetPart(firstIteration);
            if(target.partitionId != null) {
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


    public Set<Long> physicallyMigrateCopy() {
        Set<Long> logicalUserSet = new HashSet<Long>(logicalUsers.keySet());
        Set<Long> friendsSet = new HashSet<Long>();
        logicalUserSet.removeAll(idToMasterMap.keySet());
        for(Long newUid : logicalUserSet) {
            SparmesUser user = manager.getUserMasterById(newUid);
            idToMasterMap.put(newUid, user);
            friendsSet.addAll(user.getFriendIDs());
        }

        //Add friend replicas as appropriate
        for(Long friendId : friendsSet) {
            if(!idToMasterMap.containsKey(friendId) && !idToReplicaMap.containsKey(friendId)) {
                manager.addReplica(manager.getUserMasterById(friendId), id);
            }
        }
        return logicalUserSet;
    }

    public void physicallyMigrateDelete() {
        Set<Long> friendIds = new HashSet<Long>();
        Set<Long> removedUsers = new HashSet<Long>();
        for(Iterator<Long> iter = idToMasterMap.keySet().iterator(); iter.hasNext(); ) {
            Long uid = iter.next();
            SparmesUser user = idToMasterMap.get(uid);
            Long logicalPid = user.getLogicalPid();
            if(!logicalPid.equals(id)) {
                user.setMasterPartitionId(logicalPid);
                friendIds.addAll(user.getFriendIDs());
                removedUsers.add(uid);
                iter.remove();
            }
        }
        friendIds.removeAll(idToMasterMap.keySet());
        for(Long friendId : friendIds) {
            SparmesUser friend = idToMasterMap.get(friendId);
            if(friend.getReplicaPartitionIds().size() > manager.getMinNumReplicas() && !hasFriendOnPartition(friend, removedUsers)) {
                manager.removeReplica(friend, id);
            }
        }
    }

    boolean hasFriendOnPartition(SparmesUser user, Set<Long> removedUsers) {
        Set<Long> friendIds = new HashSet<Long>(user.getFriendIDs());
        friendIds.removeAll(removedUsers);
        friendIds.retainAll(idToMasterMap.keySet());
        return !friendIds.isEmpty();
    }

    public int getNumLogicalUsers() {
        return logicalUsers.size();
    }

    public void removeLogicalUser(Long userId) {
        logicalUsers.remove(userId);
    }

    public void addLogicalUser(LogicalUser logicalUser) {
        this.logicalUsers.put(logicalUser.getId(), logicalUser);
    }

    public void updateLogicalUsersPartitionWeights(Map<Long, Long> pToWeight) {
        for(LogicalUser logicalUser : logicalUsers.values()) {
            logicalUser.setpToWeight(pToWeight);
        }
    }

    public void updateLogicalUsersTotalWeights(Long totalWeight) {
        for(LogicalUser logicalUser : logicalUsers.values()) {
            logicalUser.setTotalWeight(totalWeight);
        }
    }
}
