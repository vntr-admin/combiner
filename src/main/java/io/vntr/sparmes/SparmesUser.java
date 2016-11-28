package io.vntr.sparmes;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesUser extends User {
    private Integer partitionId;
    private Integer masterPartitionId;
    private Integer logicalPid;
    private float gamma;
    private Set<Integer> replicaPartitionIds;
    private Set<Integer> logicalPartitionIds;
    private SparmesManager manager;
    private int minNumReplicas;

    public SparmesUser(Integer id, Integer initialPid, float gamma, SparmesManager manager, int minNumReplicas) {
        super(id);
        replicaPartitionIds = new HashSet<Integer>();
        logicalPartitionIds = new HashSet<Integer>();
        this.gamma = gamma;
        this.manager = manager;
        this.masterPartitionId = initialPid;
        this.partitionId = initialPid;
        this.logicalPid = initialPid;
        this.minNumReplicas = minNumReplicas;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public Integer getMasterPartitionId() {
        return masterPartitionId;
    }

    public void setMasterPartitionId(Integer masterPartitionId) {
        this.masterPartitionId = masterPartitionId;
    }

    public void addReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPartitionIds.add(replicaPartitionId);
    }

    public void removeReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPartitionIds.remove(replicaPartitionId);
    }

    public void addReplicaPartitionIds(Collection<Integer> replicaPartitionIds) {
        this.replicaPartitionIds.addAll(replicaPartitionIds);
    }

    public Set<Integer> getReplicaPartitionIds() {
        return replicaPartitionIds;
    }

    public Integer getLogicalPid() {
        return logicalPid;
    }

    public void setLogicalPid(Integer logicalPid) {
        this.logicalPid = logicalPid;
    }

    public Set<Integer> getLogicalPartitionIds() {
        return logicalPartitionIds;
    }

    public void addLogicalPartitionId(Integer pid) {
        logicalPartitionIds.add(pid);
    }

    public void removeLogicalPartitionId(Integer pid) {
        logicalPartitionIds.remove(pid);
    }

    @Override
    public SparmesUser clone() {
        SparmesUser user = new SparmesUser(getId(), masterPartitionId, gamma, manager, minNumReplicas);
        user.setMasterPartitionId(masterPartitionId);
        user.setPartitionId(partitionId);
        user.setLogicalPid(logicalPid);
        user.addReplicaPartitionIds(replicaPartitionIds);
        for (Integer friendId : getFriendIDs()) {
            user.befriend(friendId);
        }

        return user;
    }

    public boolean isReplica() {
        return !partitionId.equals(masterPartitionId);
    }

    @Override
    public String toString() {
        return super.toString() + "|M:" + masterPartitionId + "|P:" + partitionId + "|L:" + logicalPid + "|Reps:" + replicaPartitionIds.toString();
    }

    public LogicalUser getLogicalUser(boolean determineWeightsFromPhysicalPartitions) {
        Map<Integer, Integer> pToWeight = manager.getPToWeight(determineWeightsFromPhysicalPartitions);
        int totalWeight = 0;
        for(Integer pWeight: pToWeight.values()) {
            totalWeight += pWeight;
        }

        Map<Integer, Integer> pToFriendCount = getPToFriendCountLogical();

        SparmesBefriendingStrategy strat = manager.getSparmesBefriendingStrategy();

        Map<Integer, Integer> friendsToAddInEachPartition = new HashMap<Integer, Integer>();
        for(Integer pid : manager.getAllPartitionIds()) {
            friendsToAddInEachPartition.put(pid, strat.findReplicasToAddToTargetPartition(this, pid).size());
        }

        int numFriendsToDeleteInCurrentPartition = strat.findReplicasInMovingPartitionToDelete(this, Collections.<Integer>emptySet()).size();
        boolean replicateInSourcePartition = strat.shouldWeAddAReplicaOfMovingUserInMovingPartition(this, -1); //TODO: hmmnh
        return new LogicalUser(getId(), partitionId, gamma, pToFriendCount, pToWeight, replicaPartitionIds, friendsToAddInEachPartition, numFriendsToDeleteInCurrentPartition, replicateInSourcePartition, totalWeight, minNumReplicas);
    }

    Map<Integer, Integer> getFriendsToAddInEachPartitionLogical() {
        Map<Integer, Integer> friendsToAddInEachPartition = new HashMap<Integer, Integer>();
        for(Integer pid : manager.getAllPartitionIds()) {
            friendsToAddInEachPartition.put(pid, findReplicasToAddToPartition(pid).size());
        }
        return friendsToAddInEachPartition;
    }

    Set<Integer> findReplicasToAddToPartition(Integer targetPid) {
        Set<Integer> toReplicate = new HashSet<Integer>();
        for (Integer friendId : getFriendIDs()) {
            SparmesUser friend = manager.getUserMasterById(friendId);
            if (!targetPid.equals(friend.getLogicalPid()) && !friend.getLogicalPartitionIds().contains(targetPid)) {
                toReplicate.add(friendId);
            }
        }

        return toReplicate;
    }

    boolean shouldReplicateInSourcePartitionLogical() {
        for (Integer friendId : getFriendIDs()) {
            if (logicalPid.equals(manager.getUserMasterById(friendId).getLogicalPid())) {
                return true;
            }
        }

        return false;
    }

    Map<Integer, Integer> getPToFriendCountLogical() {
        Map<Integer, Integer> pToFriendCount = new HashMap<Integer, Integer>();
        for(Integer pid : manager.getAllPartitionIds()) {
            pToFriendCount.put(pid, 0);
        }
        for(Integer friendId : getFriendIDs()) {
            Integer partitionId = manager.getUserMasterById(friendId).getLogicalPid();
            pToFriendCount.put(partitionId, pToFriendCount.get(partitionId) + 1);
        }
        return pToFriendCount;
    }

    int getNumFriendsToDeleteInCurrentPartitionLogical() {
        int count = 0;
        for (Integer replicaId : findDeletionCandidatesLogical()) {
            if (manager.getUserMasterById(replicaId).getLogicalPartitionIds().size() > manager.getMinNumReplicas()) {
                count++;
            }
        }

        return count;
    }

    Set<Integer> findDeletionCandidatesLogical() {
        Set<Integer> deletionCandidates = new HashSet<Integer>();
outer:  for (Integer friendId : getFriendIDs()) {
            SparmesUser friend = manager.getUserMasterById(friendId);
            if (!friend.getLogicalPid().equals(logicalPid)) {
                for (Integer friendOfFriendId : friend.getFriendIDs()) {
                    if (friendOfFriendId.equals(getId())) {
                        continue;
                    }

                    SparmesUser friendOfFriend = manager.getUserMasterById(friendOfFriendId);
                    if (friendOfFriend.getLogicalPid().equals(logicalPid)) {
                        continue outer;
                    }
                }

                deletionCandidates.add(friendId);
            }
        }

        return deletionCandidates;
    }
}