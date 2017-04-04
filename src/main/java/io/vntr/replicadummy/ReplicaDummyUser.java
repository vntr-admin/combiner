package io.vntr.replicadummy;

import io.vntr.User;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by robertlindquist on 11/23/16.
 */


public class ReplicaDummyUser extends User {
    private Integer partitionId;
    private Integer masterPartitionId;
    private Set<Integer> replicaPartitionIds;

    public ReplicaDummyUser(Integer id) {
        super(id);
        replicaPartitionIds = new HashSet<>();
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

    @Override
    public ReplicaDummyUser clone() {
        ReplicaDummyUser user = new ReplicaDummyUser(getId());
        user.setPartitionId(partitionId);
        user.setMasterPartitionId(masterPartitionId);
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
        return super.toString() + "|masterP:" + masterPartitionId + "|P:" + partitionId + "|Reps:" + replicaPartitionIds.toString();
    }
}