package io.vntr.sparmes;

import io.vntr.User;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesUser extends User {
    private Long partitionId;
    private Long masterPartitionId;
    private Set<Long> replicaPartitionIds;

    public SparmesUser(String name, Long id) {
        super(name, id);
        replicaPartitionIds = new HashSet<Long>();
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

    @Override
    public SparmesUser clone() {
        SparmesUser user = new SparmesUser(getName(), getId());
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
}