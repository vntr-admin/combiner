package io.vntr.hermar;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

    public Target getTargetPart(boolean firstStage, boolean probabilistic) {
        if(getImbalanceFactor(pid, -1) < (2-gamma)) {
            return new Target(id, null, null, 0);
        }
        Set<Integer> targets = new HashSet<>();
        Integer maxGain = 0;
        boolean imbalanced = getImbalanceFactor(pid, 0) > gamma;
        if(imbalanced) {
            maxGain = Integer.MIN_VALUE;
        }
        for(Iterator<Integer> iter = pToFriendCount.keySet().iterator(); iter.hasNext(); ) {
            Integer targetPid = iter.next();
            if(((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) && (!probabilistic || Math.random() < 0.95)) {
                int gain = pToFriendCount.get(targetPid) - pToFriendCount.get(pid);
                if(gain > maxGain && getImbalanceFactor(targetPid, 1) < gamma) {
                    targets = new HashSet<>();
                    targets.add(targetPid);
                    maxGain = gain;
                } else if (gain == maxGain && (imbalanced || gain > 0)) {
                    targets.add(targetPid);
                }
            }
        }

        Integer target = null;
        if(!targets.isEmpty()) {
            int minWeightForPartition = Integer.MAX_VALUE;
            for(int curTarget : targets) {
                int curWeight = pToWeight.get(curTarget);
                if(curWeight < minWeightForPartition) {
                    minWeightForPartition = curWeight;
                    target = curTarget;
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

    @Override
    public String toString() {
        return "u" + id + "|, p" + pid + "|" + pToFriendCount;
    }
}
