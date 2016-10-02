package io.vntr.hermes;

import io.vntr.User;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesUser extends User {
    private Long pId;
    private double gamma;
    private Long physicalPid;
    private Long logicalPid;
    private HermesManager manager;

    public HermesUser(Long id, String name, Long initialPid, double gamma, HermesManager manager) {
        super(name, id);
        this.gamma = gamma;
        this.manager = manager;
        this.physicalPid = initialPid;
        this.logicalPid = initialPid;
    }

    public Long getpId() {
        return pId;
    }

    public void setpId(Long pId) {
        this.pId = pId;
    }

    public Long getPhysicalPid() {
        return physicalPid;
    }

    public void setPhysicalPid(Long physicalPid) {
        this.physicalPid = physicalPid;
    }

    public Long getLogicalPid() {
        return logicalPid;
    }

    public void setLogicalPid(Long logicalPid) {
        this.logicalPid = logicalPid;
    }

    public LogicalUser getLogicalUser(boolean determineWeightsFromPhysicalPartitions) {
        Map<Long, Long> pToWeight = new HashMap<Long, Long>();
        long totalWeight = 0L;
        for(Long partitionId : manager.getAllPartitionIds()) {
            int pWeight;
            if(determineWeightsFromPhysicalPartitions) {
                pWeight = manager.getPartitionById(partitionId).getNumUsers();
            }
            else {
                pWeight = manager.getPartitionById(partitionId).getNumLogicalUsers();
            }
            totalWeight += pWeight;
            pToWeight.put(partitionId, (long) pWeight);
        }
        return new LogicalUser(getId(), logicalPid, gamma, getPToFriendCount(), pToWeight, totalWeight);
    }

    Map<Long, Long> getPToFriendCount() {
        Map<Long, Long> pToFriendCount = new HashMap<Long, Long>();
        for(Long pid : manager.getAllPartitionIds()) {
            pToFriendCount.put(pid, 0L);
        }
        for(Long friendId : getFriendIDs()) {
            Long pid = manager.getUser(friendId).getLogicalPid();
            pToFriendCount.put(pid, pToFriendCount.get(pid) + 1L);
        }
        return pToFriendCount;
    }

    public void befriend(Long uid) {
        getFriendIDs().add(uid);
    }

    public void unfriend(Long uid) {
        getFriendIDs().remove(uid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HermesUser that = (HermesUser) o;

        if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) return false;
        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;
        if (pId != null ? !pId.equals(that.pId) : that.pId != null) return false;
        if (physicalPid != null ? !physicalPid.equals(that.physicalPid) : that.physicalPid != null) return false;
        if (logicalPid != null ? !logicalPid.equals(that.logicalPid) : that.logicalPid != null) return false;
        return getFriendIDs() != null ? getFriendIDs().equals(that.getFriendIDs()) : that.getFriendIDs() == null;

    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        result = 31 * result + (pId != null ? pId.hashCode() : 0);
        result = 31 * result + (physicalPid != null ? physicalPid.hashCode() : 0);
        result = 31 * result + (logicalPid != null ? logicalPid.hashCode() : 0);
        result = 31 * result + (getFriendIDs() != null ? getFriendIDs().hashCode() : 0);
        return result;
    }
}
