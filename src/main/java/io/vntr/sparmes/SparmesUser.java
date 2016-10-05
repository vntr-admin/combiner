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
    private Set<Long> logicalPartitionIds;
    private SparmesManager manager;

    public SparmesUser(String name, Long id, Long initialPid, double gamma, SparmesManager manager) {
        super(name, id);
        replicaPartitionIds = new HashSet<Long>();
        logicalPartitionIds = new HashSet<Long>();
        this.gamma = gamma;
        this.manager = manager;
        this.masterPartitionId = initialPid;
        this.partitionId = initialPid;
        this.logicalPid = initialPid;
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

    public Set<Long> getLogicalPartitionIds() {
        return logicalPartitionIds;
    }

    public void addLogicalPartitionId(Long pid) {
        logicalPartitionIds.add(pid);
    }

    public void removeLogicalPartitionId(Long pid) {
        logicalPartitionIds.remove(pid);
    }

    @Override
    public SparmesUser clone() {
        SparmesUser user = new SparmesUser(getName(), getId(), masterPartitionId, gamma, manager);
        user.setMasterPartitionId(masterPartitionId);
        user.setPartitionId(partitionId);
        user.setLogicalPid(logicalPid);
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

        Map<Long, Long> pToFriendCount = getPToFriendCountLogical();

        SparmesBefriendingStrategy strat = manager.getSparmesBefriendingStrategy();

        Map<Long, Integer> friendsToAddInEachPartition = new HashMap<Long, Integer>();
        for(Long pid : manager.getAllPartitionIds()) {
            friendsToAddInEachPartition.put(pid, strat.findReplicasToAddToTargetPartition(this, pid).size());
        }

        int numFriendsToDeleteInCurrentPartition = strat.findReplicasInMovingPartitionToDelete(this, Collections.<Long>emptySet()).size();
        boolean replicateInSourcePartition = strat.shouldWeAddAReplicaOfMovingUserInMovingPartition(this);
        return new LogicalUser(getId(), partitionId, gamma, pToFriendCount, pToWeight, replicaPartitionIds, friendsToAddInEachPartition, numFriendsToDeleteInCurrentPartition, replicateInSourcePartition, totalWeight);
    }

    Map<Long, Integer> getFriendsToAddInEachPartitionLogical() {
        Map<Long, Integer> friendsToAddInEachPartition = new HashMap<Long, Integer>();
        for(Long pid : manager.getAllPartitionIds()) {
            friendsToAddInEachPartition.put(pid, findReplicasToAddToPartition(pid).size());
        }
        return friendsToAddInEachPartition;
    }

    Set<Long> findReplicasToAddToPartition(Long targetPid) {
        Set<Long> toReplicate = new HashSet<Long>();
        for (Long friendId : getFriendIDs()) {
            SparmesUser friend = manager.getUserMasterById(friendId);
            if (!targetPid.equals(friend.getLogicalPid()) && !friend.getLogicalPartitionIds().contains(targetPid)) {
                toReplicate.add(friendId);
            }
        }

        return toReplicate;
    }

    boolean shouldReplicateInSourcePartitionLogical() {
        for (Long friendId : getFriendIDs()) {
            Long friendPid = manager.getUserMasterById(friendId).getLogicalPid();
            if (logicalPid.equals(manager.getUserMasterById(friendId).getLogicalPid())) {
                return true;
            }
        }

        return false;
    }

    Map<Long, Long> getPToFriendCountLogical() {
        Map<Long, Long> pToFriendCount = new HashMap<Long, Long>();
        for(Long pid : manager.getAllPartitionIds()) {
            pToFriendCount.put(pid, 0L);
        }
        for(Long friendId : getFriendIDs()) {
            Long partitionId = manager.getUserMasterById(friendId).getLogicalPid();
            pToFriendCount.put(partitionId, pToFriendCount.get(partitionId) + 1L);
        }
        return pToFriendCount;
    }

    int getNumFriendsToDeleteInCurrentPartitionLogical() {
        int count = 0;
        for (Long replicaId : findDeletionCandidatesLogical()) {
            if (manager.getUserMasterById(replicaId).getLogicalPartitionIds().size() > manager.getMinNumReplicas()) {
                count++;
            }
        }

        return count;
    }

    Set<Long> findDeletionCandidatesLogical() {
        Set<Long> deletionCandidates = new HashSet<Long>();
outer:  for (Long friendId : getFriendIDs()) {
            SparmesUser friend = manager.getUserMasterById(friendId);
            if (!friend.getLogicalPid().equals(logicalPid)) {
                for (Long friendOfFriendId : friend.getFriendIDs()) {
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