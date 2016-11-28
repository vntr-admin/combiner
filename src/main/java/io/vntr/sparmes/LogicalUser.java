package io.vntr.sparmes;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/29/16.
 */
public class LogicalUser {
    private int minNumReplicas;
    private Integer id;
    private Integer pid;
    private float gamma;
    private Integer totalWeight;
    private Map<Integer, Integer> pToFriendCount;
    private Map<Integer, Integer> pToWeight;

    private Set<Integer> replicaLocations;
    private Map<Integer, Integer> numFriendsToAddInEachPartition;
    private int numFriendReplicasToDeleteInSourcePartition;
    private boolean replicateInSourcePartition;

    public LogicalUser(Integer id, Integer pid, float gamma, Map<Integer, Integer> pToFriendCount, Map<Integer, Integer> pToWeight, Set<Integer> replicaLocations, Map<Integer, Integer> numFriendsToAddInEachPartition, int numFriendReplicasToDeleteInSourcePartition, boolean replicateInSourcePartition, Integer totalWeight, int minNumReplicas) {
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
        this.minNumReplicas = minNumReplicas;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    public Map<Integer, Integer> getpToFriendCount() {
        return pToFriendCount;
    }

    public void setpToFriendCount(Map<Integer, Integer> pToFriendCount) {
        this.pToFriendCount = pToFriendCount;
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

    public Target getTargetPart(boolean firstStage, boolean probabilistic) {
        if(getImbalanceFactor(pid, -1) < (2-gamma)) {
            return new Target(id, null, null, 0);
        }

        Integer target = null;
        Integer maxGain = 0;

        if(getImbalanceFactor(pid, 0) > gamma) {
            maxGain = Integer.MIN_VALUE;
        }

        for(Integer targetPid : pToFriendCount.keySet()) {
            if(((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) && (!probabilistic || Math.random() < 0.95)) {
                int gain = calculateGain(targetPid);
                if(gain > maxGain && getImbalanceFactor(targetPid, 1) < gamma) {
                    target = targetPid;
                    maxGain = gain;
                }
            }
        }
        return new Target(id, target, pid, maxGain);
    }

    //"Gain" in this instance is the reduction in replicas
    private int calculateGain(Integer targetPid) {
        boolean deleteReplicaInTargetPartition = replicaLocations.contains(targetPid) && replicaLocations.size() > minNumReplicas; //TODO: this assumes we won't add other replicas, so it's not tight
        int numToDelete = numFriendReplicasToDeleteInSourcePartition + (deleteReplicaInTargetPartition ? 1 : 0);

        int numFriendReplicasToAddInTargetPartition = numFriendsToAddInEachPartition.get(targetPid);
        int numToAdd = numFriendReplicasToAddInTargetPartition + (replicateInSourcePartition ? 1 : 0);

        return numToDelete - numToAdd;
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
        if (numFriendReplicasToDeleteInSourcePartition != that.numFriendReplicasToDeleteInSourcePartition) return false;
        if (replicateInSourcePartition != that.replicateInSourcePartition) return false;
        if (!id.equals(that.id)) return false;
        if (!pid.equals(that.pid)) return false;
        if (!totalWeight.equals(that.totalWeight)) return false;
        if (!pToFriendCount.equals(that.pToFriendCount)) return false;
        if (!pToWeight.equals(that.pToWeight)) return false;
        if (!replicaLocations.equals(that.replicaLocations)) return false;
        return numFriendsToAddInEachPartition.equals(that.numFriendsToAddInEachPartition);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + pid.hashCode();
        result = 31 * result + (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
        result = 31 * result + totalWeight.hashCode();
        result = 31 * result + pToFriendCount.hashCode();
        result = 31 * result + pToWeight.hashCode();
        result = 31 * result + replicaLocations.hashCode();
        result = 31 * result + numFriendsToAddInEachPartition.hashCode();
        result = 31 * result + numFriendReplicasToDeleteInSourcePartition;
        result = 31 * result + (replicateInSourcePartition ? 1 : 0);
        return result;
    }

    public void setPToFriendCount(Map<Integer, Integer> updatedFriendCounts) {
        pToFriendCount = updatedFriendCounts;
    }
}
