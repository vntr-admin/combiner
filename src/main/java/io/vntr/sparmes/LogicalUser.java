package io.vntr.sparmes;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/29/16.
 */
public class LogicalUser {
    private Long id;
    private Long pid;
    private double gamma;
    private Long totalWeight;
    private Map<Long, Long> pToFriendCount;
    private Map<Long, Long> pToWeight;

    private Set<Long> replicaLocations;
    private Map<Long, Integer> numFriendsToAddInEachPartition;
    private int numFriendReplicasToDeleteInSourcePartition;
    private boolean replicateInSourcePartition;

    public LogicalUser(Long id, Long pid, double gamma, Map<Long, Long> pToFriendCount, Map<Long, Long> pToWeight, Set<Long> replicaLocations, Map<Long, Integer> numFriendsToAddInEachPartition, int numFriendReplicasToDeleteInSourcePartition, boolean replicateInSourcePartition, Long totalWeight) {
        this.id = id;
        this.pid = pid;
        this.gamma = gamma;
        this.pToFriendCount = pToFriendCount;
        this.pToWeight = pToWeight;
        this.replicaLocations = replicaLocations;
        this.numFriendsToAddInEachPartition = numFriendsToAddInEachPartition;
        this.numFriendReplicasToDeleteInSourcePartition = numFriendReplicasToDeleteInSourcePartition;
        this.replicateInSourcePartition = replicateInSourcePartition;
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
                int gain = calculateGain(targetPid);
                if(gain > maxGain && getImbalanceFactor(targetPid, 1L) < gamma) {
                    target = targetPid;
                    maxGain = gain;
                }
            }
        }
        return new Target(id, target, pid, maxGain);
    }

    //"Gain" in this instance is the reduction in replicas
    private int calculateGain(Long targetPid) {
        boolean deleteReplicaInTargetPartition = replicaLocations.contains(targetPid);
        int numToDelete = numFriendReplicasToDeleteInSourcePartition + (deleteReplicaInTargetPartition ? 1 : 0);

        int numFriendReplicasToAddInTargetPartition = numFriendsToAddInEachPartition.get(targetPid);
        int numToAdd = numFriendReplicasToAddInTargetPartition + (replicateInSourcePartition ? 1 : 0);

        return numToDelete - numToAdd;
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
        if (numFriendReplicasToDeleteInSourcePartition != that.numFriendReplicasToDeleteInSourcePartition) return false;
        if (replicateInSourcePartition != that.replicateInSourcePartition) return false;
        if (!id.equals(that.id)) return false;
        if (!pid.equals(that.pid)) return false;
        if (!pToFriendCount.equals(that.pToFriendCount)) return false;
        if (!pToWeight.equals(that.pToWeight)) return false;
        if (!replicaLocations.equals(that.replicaLocations)) return false;
        if (!numFriendsToAddInEachPartition.equals(that.numFriendsToAddInEachPartition)) return false;
        return totalWeight.equals(that.totalWeight);

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id.hashCode();
        result = 31 * result + pid.hashCode();
        temp = Double.doubleToLongBits(gamma);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + pToFriendCount.hashCode();
        result = 31 * result + pToWeight.hashCode();
        result = 31 * result + replicaLocations.hashCode();
        result = 31 * result + numFriendsToAddInEachPartition.hashCode();
        result = 31 * result + numFriendReplicasToDeleteInSourcePartition;
        result = 31 * result + (replicateInSourcePartition ? 1 : 0);
        result = 31 * result + totalWeight.hashCode();
        return result;
    }

    public void setPToFriendCount(Map<Long, Long> updatedFriendCounts) {
        pToFriendCount = updatedFriendCounts;
    }
}
