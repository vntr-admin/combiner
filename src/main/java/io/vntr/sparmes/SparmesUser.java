package io.vntr.sparmes;

import io.vntr.RepUser;
import io.vntr.User;

import java.util.*;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesUser extends RepUser {
    private float gamma;
    private Integer logicalPid;
    private Set<Integer> logicalPids;
    private SparmesManager manager;
    private int minNumReplicas;

    public SparmesUser(Integer id, Integer initialPid, float gamma, SparmesManager manager, int minNumReplicas) {
        super(id, initialPid);
        logicalPids = new HashSet<>();
        this.gamma = gamma;
        this.manager = manager;
        this.logicalPid = initialPid;
        this.minNumReplicas = minNumReplicas;
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
        SparmesUser user = new SparmesUser(getId(), getBasePid(), gamma, manager, minNumReplicas);
        user.setBasePid(getBasePid());
        user.setLogicalPid(logicalPid);
        user.addReplicaPartitionIds(getReplicaPids());
        for (Integer friendId : getFriendIDs()) {
            user.befriend(friendId);
        }

        return user;
    }

    @Override
    public String toString() {
        return super.toString() + "|L:" + logicalPid + "|LReps:" + logicalPids.toString();
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
        return new LogicalUser(getId(), getBasePid(), gamma, pToFriendCount, pToWeight, getReplicaPids(), friendsToAddInEachPartition, numFriendsToDeleteInCurrentPartition, replicateInSourcePartition, totalWeight, minNumReplicas);
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
        if (!super.equals(o)) return false;

        SparmesUser that = (SparmesUser) o;

        if (Float.compare(that.gamma, gamma) != 0) return false;
        if (minNumReplicas != that.minNumReplicas) return false;
        if (logicalPid != null ? !logicalPid.equals(that.logicalPid) : that.logicalPid != null) return false;
        if (logicalPids != null ? !logicalPids.equals(that.logicalPids) : that.logicalPids != null) return false;
        return manager != null ? manager.equals(that.manager) : that.manager == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (logicalPid != null ? logicalPid.hashCode() : 0);
        result = 31 * result + (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
        result = 31 * result + (logicalPids != null ? logicalPids.hashCode() : 0);
        result = 31 * result + (manager != null ? manager.hashCode() : 0);
        result = 31 * result + minNumReplicas;
        return result;
    }
}
