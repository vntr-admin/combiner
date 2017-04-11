package io.vntr.hermar;

import io.vntr.User;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermarUser extends User {
    private Integer pId;
    private float gamma;
    private Integer physicalPid;
    private Integer logicalPid;
    private HermarManager manager;

    public HermarUser(Integer id, Integer initialPid, float gamma, HermarManager manager) {
        super(id);
        this.gamma = gamma;
        this.manager = manager;
        this.physicalPid = initialPid;
        this.logicalPid = initialPid;
    }

    public Integer getpId() {
        return pId;
    }

    public void setpId(Integer pId) {
        this.pId = pId;
    }

    public Integer getPhysicalPid() {
        return physicalPid;
    }

    public void setPhysicalPid(Integer physicalPid) {
        this.physicalPid = physicalPid;
    }

    public Integer getLogicalPid() {
        return logicalPid;
    }

    public void setLogicalPid(Integer logicalPid) {
        this.logicalPid = logicalPid;
        manager.updateLogicalPidCache(getId(), logicalPid);
    }

    public LogicalUser getLogicalUser(boolean determineWeightsFromPhysicalPartitions) {
        Map<Integer, Integer> pToWeight = new HashMap<>();
        int totalWeight = 0;
        for(Integer partitionId : manager.getAllPartitionIds()) {
            int pWeight;
            if(determineWeightsFromPhysicalPartitions) {
                pWeight = manager.getPartitionById(partitionId).getNumUsers();
            }
            else {
                pWeight = manager.getPartitionById(partitionId).getNumLogicalUsers();
            }
            totalWeight += pWeight;
            pToWeight.put(partitionId, pWeight);
        }
        return new LogicalUser(getId(), logicalPid, gamma, getPToFriendCount(), pToWeight, totalWeight);
    }

    Map<Integer, Integer> getPToFriendCount() {
        Map<Integer, Integer> pToFriendCount = new HashMap<>();
        for(Integer pid : manager.getAllPartitionIds()) {
            pToFriendCount.put(pid, 0);
        }
        for(Integer friendId : getFriendIDs()) {
            Integer pid = manager.getLogicalPartitionIdForUser(friendId);
            pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
        }
        return pToFriendCount;
    }

    public void befriend(Integer uid) {
        getFriendIDs().add(uid);
    }

    public void unfriend(Integer uid) {
        getFriendIDs().remove(uid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HermarUser user = (HermarUser) o;

        if (Float.compare(user.gamma, gamma) != 0) return false;
        if (!pId.equals(user.pId)) return false;
        if (!physicalPid.equals(user.physicalPid)) return false;
        return logicalPid.equals(user.logicalPid);

    }

    @Override
    public int hashCode() {
        int result = pId.hashCode();
        result = 31 * result + (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
        result = 31 * result + physicalPid.hashCode();
        result = 31 * result + logicalPid.hashCode();
        return result;
    }
}
