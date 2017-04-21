package io.vntr.hermar;

import io.vntr.User;

import java.util.HashMap;
import java.util.Map;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermarUser extends User {
    private float gamma;
    private Integer logicalPid;
    private HermarManager manager;

    public HermarUser(Integer id, Integer initialPid, float gamma, HermarManager manager) {
        super(id, initialPid);
        this.gamma = gamma;
        this.manager = manager;
        this.logicalPid = initialPid;
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
        if (!(o instanceof HermarUser)) return false;
        if (!super.equals(o)) return false;

        HermarUser that = (HermarUser) o;

        if (Float.compare(that.gamma, gamma) != 0) return false;
        if (logicalPid != null ? !logicalPid.equals(that.logicalPid) : that.logicalPid != null) return false;
        return manager != null ? manager.equals(that.manager) : that.manager == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
        result = 31 * result + (logicalPid != null ? logicalPid.hashCode() : 0);
        result = 31 * result + (manager != null ? manager.hashCode() : 0);
        return result;
    }
}
