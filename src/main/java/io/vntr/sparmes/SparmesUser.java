package io.vntr.sparmes;

import io.vntr.User;

import java.util.*;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesUser extends User {
    private Integer masterPid;
    private Integer logicalPid;
    private float gamma;
    private Set<Integer> replicaPids;
    private Set<Integer> logicalPids;
    private SparmesManager manager;
    private int minNumReplicas;

    public SparmesUser(Integer id, Integer initialPid, float gamma, SparmesManager manager, int minNumReplicas) {
        super(id);
        replicaPids = new HashSet<>();
        logicalPids = new HashSet<>();
        this.gamma = gamma;
        this.manager = manager;
        this.masterPid = initialPid;
        this.logicalPid = initialPid;
        this.minNumReplicas = minNumReplicas;
    }

    public Integer getMasterPid() {
        return masterPid;
    }

    public void setMasterPid(Integer masterPid) {
        this.masterPid = masterPid;
    }

    public void addReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPids.add(replicaPartitionId);
    }

    public void removeReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPids.remove(replicaPartitionId);
    }

    public void addReplicaPartitionIds(Collection<Integer> replicaPartitionIds) {
        this.replicaPids.addAll(replicaPartitionIds);
    }

    public Set<Integer> getReplicaPids() {
        return replicaPids;
    }

    public Integer getLogicalPid() {
        return logicalPid;
    }

    public void setLogicalPid(Integer logicalPid) {
        this.logicalPid = logicalPid;
    }

    public Set<Integer> getLogicalPids() {
        return logicalPids;
    }

    public void addLogicalPartitionId(Integer pid) {
        logicalPids.add(pid);
    }

    public void removeLogicalPartitionId(Integer pid) {
        logicalPids.remove(pid);
    }

    @Override
    public SparmesUser clone() {
        SparmesUser user = new SparmesUser(getId(), masterPid, gamma, manager, minNumReplicas);
        user.setMasterPid(masterPid);
        user.setLogicalPid(logicalPid);
        user.addReplicaPartitionIds(replicaPids);
        for (Integer friendId : getFriendIDs()) {
            user.befriend(friendId);
        }

        return user;
    }

    @Override
    public String toString() {
        return super.toString() + "|M:" + masterPid + "|L:" + logicalPid + "|Reps:" + replicaPids.toString();
    }

    public LogicalUser getLogicalUser(boolean determineWeightsFromPhysicalPartitions) {
        Map<Integer, Integer> pToWeight = manager.getPToWeight(determineWeightsFromPhysicalPartitions);
        int totalWeight = 0;
        for(Integer pWeight: pToWeight.values()) {
            totalWeight += pWeight;
        }

        Map<Integer, Integer> pToFriendCount = getPToFriendCountLogical();

        SparmesBefriendingStrategy strat = manager.getSparmesBefriendingStrategy();

        Map<Integer, Integer> friendsToAddInEachPartition = new HashMap<>();
        for(Integer pid : manager.getAllPartitionIds()) {
            friendsToAddInEachPartition.put(pid, strat.findReplicasToAddToTargetPartition(this, pid).size());
        }

        int numFriendsToDeleteInCurrentPartition = strat.findReplicasInMovingPartitionToDelete(this, Collections.<Integer>emptySet()).size();
        boolean replicateInSourcePartition = strat.shouldWeAddAReplicaOfMovingUserInMovingPartition(this, -1); //TODO: hmmnh
        return new LogicalUser(getId(), masterPid, gamma, pToFriendCount, pToWeight, replicaPids, friendsToAddInEachPartition, numFriendsToDeleteInCurrentPartition, replicateInSourcePartition, totalWeight, minNumReplicas);
    }

    Map<Integer, Integer> getFriendsToAddInEachPartitionLogical() {
        Map<Integer, Integer> friendsToAddInEachPartition = new HashMap<>();
        for(Integer pid : manager.getAllPartitionIds()) {
            friendsToAddInEachPartition.put(pid, findHowManyReplicasToAddToPartition(pid));
        }
        return friendsToAddInEachPartition;
    }

    int findHowManyReplicasToAddToPartition(Integer targetPid) {
        int count = 0;
        for (Integer friendId : getFriendIDs()) {
            SparmesUser friend = manager.getUserMasterById(friendId);
            if (!targetPid.equals(friend.getLogicalPid()) && !friend.getLogicalPids().contains(targetPid)) {
                count++;
            }
        }
        return count;
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
        Map<Integer, Integer> pToFriendCount = new HashMap<>();
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
            if (manager.getUserMasterById(replicaId).getLogicalPids().size() > manager.getMinNumReplicas()) {
                count++;
            }
        }

        return count;
    }

    Set<Integer> findDeletionCandidatesLogical() {
        Set<Integer> deletionCandidates = new HashSet<>();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparmesUser)) return false;

        SparmesUser that = (SparmesUser) o;

        return  this.minNumReplicas == that.minNumReplicas
                && (Float.compare(that.gamma, gamma) == 0)
                && safeEquals(this.masterPid, that.masterPid)
                && safeEquals(this.logicalPid, that.logicalPid)
                && safeEquals(this.getId(), that.getId())
                && safeEquals(this.replicaPids, that.replicaPids)
                && safeEquals(this.logicalPids, that.logicalPids)
                && safeEquals(this.getFriendIDs(), that.getFriendIDs());
    }

    @Override
    public int hashCode() {
        int result = minNumReplicas;
        result = 31 * result + (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
        result = 31 * result + safeHashCode(masterPid);
        result = 31 * result + safeHashCode(logicalPid);
        result = 31 * result + safeHashCode(getId());
        result = 31 * result + safeHashCode(replicaPids);
        result = 31 * result + safeHashCode(logicalPids);
        result = 31 * result + safeHashCode(getFriendIDs());
        return result;
    }
}
