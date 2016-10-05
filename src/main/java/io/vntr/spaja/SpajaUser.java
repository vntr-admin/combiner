package io.vntr.spaja;

import io.vntr.User;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by robertlindquist on 9/28/16.
 * Should be done
 */
public class SpajaUser extends User {
    private float alpha;
    private Integer partitionId;
    private Integer masterPartitionId;
    private Set<Integer> replicaPartitionIds;
    private SpajaManager manager; //TODO: this is sloppy; replace its usage with passing in the necessary info
    private int k;

    public SpajaUser(Integer id, float alpha, int k, SpajaManager manager) {
        super(id);
        replicaPartitionIds = new HashSet<Integer>();
        this.alpha = alpha;
        this.k = k;
        this.manager = manager;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public Integer getMasterPartitionId() {
        return masterPartitionId;
    }

    public void setMasterPartitionId(Integer masterPartitionId) {
        this.masterPartitionId = masterPartitionId;
    }

    public void addReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPartitionIds.add(replicaPartitionId);
    }

    public void removeReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPartitionIds.remove(replicaPartitionId);
    }

    public void addReplicaPartitionIds(Collection<Integer> replicaPartitionIds) {
        this.replicaPartitionIds.addAll(replicaPartitionIds);
    }

    public Set<Integer> getReplicaPartitionIds() {
        return replicaPartitionIds;
    }

    @Override
    public SpajaUser clone() {
        SpajaUser user = new SpajaUser(getId(), alpha, k, manager);
        user.setPartitionId(partitionId);
        user.setMasterPartitionId(masterPartitionId);
        user.addReplicaPartitionIds(replicaPartitionIds);
        for (Integer friendId : getFriendIDs()) {
            user.befriend(friendId);
        }

        return user;
    }

    public boolean isReplica() {
        return !partitionId.equals(masterPartitionId);
    }

    @Override
    public String toString() {
        return super.toString() + "|masterP:" + masterPartitionId + "|P:" + partitionId + "|Reps:" + replicaPartitionIds.toString();
    }


    public SpajaUser findPartner(Collection<SpajaUser> randomSamplingOfUsers, float t, SpajaBefriendingStrategy strategy) {
        SpajaUser bestPartner = null;
        float bestScore = 0;

        for(SpajaUser partner : randomSamplingOfUsers) {
            int replicasOnMine   = manager.getPartitionById(partitionId).getNumReplicas();
            int replicasOnTheirs = manager.getPartitionById(partner.getPartitionId()).getNumReplicas();

            float oldScore = (float)(Math.pow(replicasOnMine, alpha) + Math.pow(replicasOnTheirs, alpha));
            float newScore = getReplicasIfSwappedUsingAlpha(this, partner, strategy);

            if(newScore > bestScore && (newScore * t) > oldScore) {
                bestPartner = partner;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    float getReplicasIfSwappedUsingAlpha(SpajaUser u1, SpajaUser u2, SpajaBefriendingStrategy strategy) {
        SwapChanges swapChanges = strategy.getSwapChanges(u1, u2);

        int replicasInP1 = manager.getPartitionById(u1.getPartitionId()).getNumReplicas();
        int replicasInP2 = manager.getPartitionById(u2.getPartitionId()).getNumReplicas();

        replicasInP1 += swapChanges.getAddToP1().size();
        replicasInP2 += swapChanges.getAddToP2().size();

        replicasInP1 -= swapChanges.getRemoveFromP1().size();
        replicasInP2 -= swapChanges.getRemoveFromP2().size();

        return (float)(Math.pow(replicasInP1, alpha) + Math.pow(replicasInP2, alpha));
    }
}