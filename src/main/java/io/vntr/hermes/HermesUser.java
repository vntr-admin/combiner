package io.vntr.hermes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesUser {
    private Long id;
    private String name;
    private Long pId;
    private double gamma;
    private Long physicalPid;
    private Long logicalPid;
    private HermesManager manager;
    private Set<Long> friendIds;

    public HermesUser(Long id, String name, Long initialPid, double gamma, HermesManager manager) {
        this.id = id;
        this.gamma = gamma;
        this.manager = manager;
        this.physicalPid = initialPid;
        this.logicalPid = initialPid;
        this.friendIds = new HashSet<Long>();
    }

    public Long getId() {
        return id;
    }

    public HermesUser(String name) {
        this.name = name;
    }

    public Set<Long> getFriendIds() {
        return friendIds;
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

    public LogicalUser getLogicalUser() {
        Map<Long, Long> pToWeight = new HashMap<Long, Long>();
        long totalWeight = 0L;
        for(Long partitionId : manager.getAllPartitionIds()) {
            int pWeight = manager.getPartitionById(partitionId).getNumLogicalUsers();
            totalWeight += pWeight;
            pToWeight.put(partitionId, (long) pWeight);
        }
        Map<Long, Long> pToFriendCount = new HashMap<Long, Long>();
        for(Long friendId : friendIds) {
            HermesPartition physPartition = manager.getPartitionById(manager.getPartitionIdForUser(friendId));
            Long partitionId = physPartition.getUserById(friendId).getLogicalPid();
            if(!pToFriendCount.containsKey(partitionId)) {
                pToFriendCount.put(partitionId, 1L);
            }
            else {
                pToFriendCount.put(partitionId, pToFriendCount.get(partitionId) + 1L);
            }
        }
        return new LogicalUser(id, logicalPid, gamma, pToFriendCount, pToWeight, totalWeight);
    }

    public void befriend(Long uid) {
        friendIds.add(uid);
    }

    public void unfriend(Long uid) {
        friendIds.remove(uid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HermesUser that = (HermesUser) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (pId != null ? !pId.equals(that.pId) : that.pId != null) return false;
        if (physicalPid != null ? !physicalPid.equals(that.physicalPid) : that.physicalPid != null) return false;
        if (logicalPid != null ? !logicalPid.equals(that.logicalPid) : that.logicalPid != null) return false;
        return friendIds != null ? friendIds.equals(that.friendIds) : that.friendIds == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (pId != null ? pId.hashCode() : 0);
        result = 31 * result + (physicalPid != null ? physicalPid.hashCode() : 0);
        result = 31 * result + (logicalPid != null ? logicalPid.hashCode() : 0);
        result = 31 * result + (friendIds != null ? friendIds.hashCode() : 0);
        return result;
    }
}
