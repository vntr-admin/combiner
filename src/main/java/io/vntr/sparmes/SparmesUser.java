package io.vntr.sparmes;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesUser extends User {
    private Long partitionId;
    private Long masterPartitionId;
    private Long logicalPid;
    private double gamma;
    private Set<Long> replicaPartitionIds;
    private SparmesManager manager;

    public SparmesUser(String name, Long id, double gamma, SparmesManager manager) {
        super(name, id);
        replicaPartitionIds = new HashSet<Long>();
        this.gamma = gamma;
        this.manager = manager;
    }

    public Long getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Long partitionId) {
        this.partitionId = partitionId;
    }

    public Long getMasterPartitionId() {
        return masterPartitionId;
    }

    public void setMasterPartitionId(Long masterPartitionId) {
        this.masterPartitionId = masterPartitionId;
    }

    public void addReplicaPartitionId(Long replicaPartitionId) {
        this.replicaPartitionIds.add(replicaPartitionId);
    }

    public void removeReplicaPartitionId(Long replicaPartitionId) {
        this.replicaPartitionIds.remove(replicaPartitionId);
    }

    public void addReplicaPartitionIds(Collection<Long> replicaPartitionIds) {
        this.replicaPartitionIds.addAll(replicaPartitionIds);
    }

    public Set<Long> getReplicaPartitionIds() {
        return replicaPartitionIds;
    }

    public Long getLogicalPid() {
        return logicalPid;
    }

    public void setLogicalPid(Long logicalPid) {
        this.logicalPid = logicalPid;
    }

    @Override
    public SparmesUser clone() {
        SparmesUser user = new SparmesUser(getName(), getId(), gamma, manager);
        user.setPartitionId(partitionId);
        user.setMasterPartitionId(masterPartitionId);
        user.addReplicaPartitionIds(replicaPartitionIds);
        for (Long friendId : getFriendIDs()) {
            user.befriend(friendId);
        }

        return user;
    }

    public boolean isReplica() {
        return !partitionId.equals(masterPartitionId);
    }

    @Override
    public String toString() {
        return super.toString() + "|masterP:" + masterPartitionId + "|P:" + partitionId + "|Reps:" + replicaPartitionIds.toString();
    }

    public LogicalUser getLogicalUser(boolean determineWeightsFromPhysicalPartitions) {
        Map<Long, Long> pToWeight = manager.getPToWeight(determineWeightsFromPhysicalPartitions);
        long totalWeight = 0L;
        for(Long pWeight: pToWeight.values()) {
            totalWeight += pWeight;
        }

        Map<Long, Long> pToFriendCount = getPToFriendCount();

        SparmesBefriendingStrategy strat = manager.getSparmesBefriendingStrategy();

        Map<Long, Integer> friendsToAddInEachPartition = new HashMap<Long, Integer>();
        for(Long pid : manager.getAllPartitionIds()) {
            friendsToAddInEachPartition.put(pid, strat.findReplicasToAddToTargetPartition(this, pid).size());
        }

        int numFriendsToDeleteInCurrentPartition = strat.findReplicasInMovingPartitionToDelete(this, Collections.<Long>emptySet()).size();
        boolean replicateInSourcePartition = strat.shouldWeAddAReplicaOfMovingUserInMovingPartition(this);
        return new LogicalUser(getId(), partitionId, gamma, pToFriendCount, pToWeight, replicaPartitionIds, friendsToAddInEachPartition, numFriendsToDeleteInCurrentPartition, replicateInSourcePartition, totalWeight);
    }

    Map<Long, Long> getPToFriendCount() {
        Map<Long, Long> pToFriendCount = new HashMap<Long, Long>();
        for(Long pid : manager.getAllPartitionIds()) {
            pToFriendCount.put(pid, 0L);
        }
        for(Long friendId : getFriendIDs()) {
            SparmesUser friend = manager.getUserMasterById(friendId);
            Long partitionId = friend.getLogicalPid();
            pToFriendCount.put(partitionId, pToFriendCount.get(partitionId) + 1L);
        }
        return pToFriendCount;
    }

}