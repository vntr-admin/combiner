package io.vntr.hermes;

import io.vntr.User;

import java.util.HashMap;
import java.util.Map;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesUser extends User {
    private float gamma;
    private Integer physicalPid;
    private Integer logicalPid;
    private HermesManager manager;

    public HermesUser(Integer id, Integer initialPid, float gamma, HermesManager manager) {
        super(id);
        this.gamma = gamma;
        this.manager = manager;
        this.physicalPid = initialPid;
        this.logicalPid = initialPid;
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

    //TODO: you may as well move this junk into a new HermesRepartitioner class
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

        HermesUser that = (HermesUser) o;

        return  (Float.compare(that.gamma, gamma) == 0)
                && safeEquals(this.physicalPid, that.physicalPid)
                && safeEquals(this.logicalPid,  that.logicalPid)
                && safeEquals(this.getId(), that.getId())
                && safeEquals(this.getFriendIDs(), that.getFriendIDs());
    }

    @Override
    public int hashCode() {
        int result = (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
        result = 31 * result + safeHashCode(physicalPid);
        result = 31 * result + safeHashCode(logicalPid);
        result = 31 * result + safeHashCode(getId());
        result = 31 * result + safeHashCode(getFriendIDs());
        return result;
    }
}
