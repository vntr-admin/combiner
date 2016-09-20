package io.vntr.hermes;

import java.util.Map;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class LogicalUser {
    private Long id;
    private Long pid;
    private double gamma;
    private Map<Long, Long> pToFriendCount;
    private Map<Long, Long> pToWeight;
    private Long totalWeight;

    public LogicalUser(Long id, Long pid, double gamma, Map<Long, Long> pToFriendCount, Map<Long, Long> pToWeight, Long totalWeight) {
        this.id = id;
        this.pid = pid;
        this.gamma = gamma;
        this.pToFriendCount = pToFriendCount;
        this.pToWeight = pToWeight;
        this.totalWeight = totalWeight;
    }

    public Long getId() {
        return id;
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
        if(getImbalanceFactor(pid, -1L) < (2-gamma)) {
            return new Target(id, null, null, 0);
        }
        Long target = null;
        Integer maxGain = 0;
        if(getImbalanceFactor(pid, 0L) > gamma) {
            maxGain = Integer.MIN_VALUE;
        }
        for(Long targetPid : pToFriendCount.keySet()) {
            if((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) {
                int gain = (int) (pToFriendCount.get(targetPid) - pToFriendCount.get(pid));
                if(gain > maxGain && getImbalanceFactor(targetPid, 1L) < gamma) {
                    target = targetPid;
                    maxGain = gain;
                }
            }
        }
        return new Target(id, target, pid, maxGain);
    }

    private double getImbalanceFactor(Long pId, Long offset) {
        double partitionWeight = pToWeight.get(pId) + offset;
        double averageWeight = ((double) totalWeight) / pToWeight.keySet().size();
        return partitionWeight / averageWeight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogicalUser that = (LogicalUser) o;

        if (Double.compare(that.gamma, gamma) != 0) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (pid != null ? !pid.equals(that.pid) : that.pid != null) return false;
        if (pToFriendCount != null ? !pToFriendCount.equals(that.pToFriendCount) : that.pToFriendCount != null)
            return false;
        if (pToWeight != null ? !pToWeight.equals(that.pToWeight) : that.pToWeight != null) return false;
        return totalWeight != null ? totalWeight.equals(that.totalWeight) : that.totalWeight == null;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id != null ? id.hashCode() : 0;
        result = 31 * result + (pid != null ? pid.hashCode() : 0);
        temp = Double.doubleToLongBits(gamma);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (pToFriendCount != null ? pToFriendCount.hashCode() : 0);
        result = 31 * result + (pToWeight != null ? pToWeight.hashCode() : 0);
        result = 31 * result + (totalWeight != null ? totalWeight.hashCode() : 0);
        return result;
    }
}
