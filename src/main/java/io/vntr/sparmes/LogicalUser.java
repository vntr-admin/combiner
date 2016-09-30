package io.vntr.sparmes;

import java.util.Map;

/**
 * Created by robertlindquist on 9/29/16.
 */
public class LogicalUser {
    private Long id;
    private Long pid;
    private Map<Long, Long> pToFriendCount;
    private Map<Long, Long> pToWeight;
    private Long totalWeight;

    public LogicalUser(Long id, Long pid, Map<Long, Long> pToFriendCount, Map<Long, Long> pToWeight, Long totalWeight) {
        this.id = id;
        this.pid = pid;
        this.pToFriendCount = pToFriendCount;
        this.pToWeight = pToWeight;
        this.totalWeight = totalWeight;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Map<Long, Long> getpToFriendCount() {
        return pToFriendCount;
    }

    public void setpToFriendCount(Map<Long, Long> pToFriendCount) {
        this.pToFriendCount = pToFriendCount;
    }

    public Map<Long, Long> getpToWeight() {
        return pToWeight;
    }

    public void setpToWeight(Map<Long, Long> pToWeight) {
        this.pToWeight = pToWeight;
    }

    public Long getTotalWeight() {
        return totalWeight;
    }

    public void setTotalWeight(Long totalWeight) {
        this.totalWeight = totalWeight;
    }

    public Target getTargetPart(boolean firstStage) {
        //TODO: think about if and how to incorporate Hermes' notion of gamma
        return null;
    }

    //TODO: this will need to be updated if you add more fields
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogicalUser that = (LogicalUser) o;

        if (!id.equals(that.id)) return false;
        if (!pid.equals(that.pid)) return false;
        if (!pToFriendCount.equals(that.pToFriendCount)) return false;
        if (!pToWeight.equals(that.pToWeight)) return false;
        return totalWeight.equals(that.totalWeight);

    }

    //TODO: this will need to be updated if you add more fields
    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + pid.hashCode();
        result = 31 * result + pToFriendCount.hashCode();
        result = 31 * result + pToWeight.hashCode();
        result = 31 * result + totalWeight.hashCode();
        return result;
    }
}
