package io.vntr.hermes;

import java.util.Map;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class LogicalUser {
    private Integer id;
    private Integer pid;
    private float gamma;
    private Map<Integer, Integer> pToFriendCount;
    private Map<Integer, Integer> pToWeight;
    private Integer totalWeight;

    public LogicalUser(Integer id, Integer pid, float gamma, Map<Integer, Integer> pToFriendCount, Map<Integer, Integer> pToWeight, Integer totalWeight) {
        this.id = id;
        this.pid = pid;
        this.gamma = gamma;
        this.pToFriendCount = pToFriendCount;
        this.pToWeight = pToWeight;
        this.totalWeight = totalWeight;
    }

    public Integer getId() {
        return id;
    }

    public Map<Integer, Integer> getpToWeight() {
        return pToWeight;
    }

    public void setpToWeight(Map<Integer, Integer> pToWeight) {
        this.pToWeight = pToWeight;
    }

    public Integer getTotalWeight() {
        return totalWeight;
    }

    public void setTotalWeight(Integer totalWeight) {
        this.totalWeight = totalWeight;
    }

    public Integer getPid() {
        return pid;
    }

    public Map<Integer, Integer> getpToFriendCount() {
        return pToFriendCount;
    }

    public Target getTargetPart(boolean firstStage) {
        if(getImbalanceFactor(pid, -1) < (2-gamma)) {
            return new Target(id, null, null, 0);
        }
        Integer target = null;
        Integer maxGain = 0;
        if(getImbalanceFactor(pid, 0) > gamma) {
            maxGain = Integer.MIN_VALUE;
        }
        for(Integer targetPid : pToFriendCount.keySet()) {
            if((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) {
                int gain = (int) (pToFriendCount.get(targetPid) - pToFriendCount.get(pid));
                if(gain > maxGain && getImbalanceFactor(targetPid, 1) < gamma) {
                    target = targetPid;
                    maxGain = gain;
                }
            }
        }
        return new Target(id, target, pid, maxGain);
    }

    private float getImbalanceFactor(Integer pId, Integer offset) {
        float partitionWeight = pToWeight.get(pId) + offset;
        float averageWeight = ((float) totalWeight) / pToWeight.keySet().size();
        return partitionWeight / averageWeight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogicalUser that = (LogicalUser) o;

        if (Float.compare(that.gamma, gamma) != 0) return false;
        if (!id.equals(that.id)) return false;
        if (!pid.equals(that.pid)) return false;
        if (!pToFriendCount.equals(that.pToFriendCount)) return false;
        if (!pToWeight.equals(that.pToWeight)) return false;
        return totalWeight.equals(that.totalWeight);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + pid.hashCode();
        result = 31 * result + (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
        result = 31 * result + pToFriendCount.hashCode();
        result = 31 * result + pToWeight.hashCode();
        result = 31 * result + totalWeight.hashCode();
        return result;
    }

    public void setPToFriendCount(Map<Integer, Integer> updatedFriendCounts) {
        pToFriendCount = updatedFriendCounts;
    }
}
