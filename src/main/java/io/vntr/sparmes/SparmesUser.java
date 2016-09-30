package io.vntr.sparmes;

import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 9/28/16.
 */
public class SparmesUser extends User {
    private Long partitionId;
    private Long masterPartitionId;
    private Set<Long> replicaPartitionIds;
    private SparmesManager manager;

    public SparmesUser(String name, Long id, SparmesManager manager) {
        super(name, id);
        replicaPartitionIds = new HashSet<Long>();
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

    @Override
    public SparmesUser clone() {
        SparmesUser user = new SparmesUser(getName(), getId(), manager);
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
        Map<Long, Long> pToWeight = new HashMap<Long, Long>();
        long totalWeight = 0L;
        for(Long partitionId : manager.getAllPartitionIds()) {
            int pWeight;
            if(determineWeightsFromPhysicalPartitions) {
                pWeight = manager.getPartitionById(partitionId).getNumMasters();
            }
            else {
                pWeight = manager.getPartitionById(partitionId).getNumLogicalUsers();
            }
            totalWeight += pWeight;
            pToWeight.put(partitionId, (long) pWeight);
        }
        Map<Long, Long> pToFriendCount = new HashMap<Long, Long>();
        for(Long pid : manager.getAllPartitionIds()) {
            pToFriendCount.put(pid, 0L);
        }
        //TODO: fill out pToFriendCount
        //TODO: figure out all the other stuff we need here

        return new LogicalUser(getId(), partitionId, pToFriendCount, pToWeight, totalWeight);
    }
}