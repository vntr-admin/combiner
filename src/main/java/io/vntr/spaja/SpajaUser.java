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
        return super.toString() + "|M:" + masterPartitionId + "|P:" + partitionId + "|R:" + replicaPartitionIds.toString();
    }


    public SpajaUser findPartner(Collection<SpajaUser> randomSamplingOfUsers, float t, SpajaBefriendingStrategy strategy) {
        SpajaUser bestPartner = null;
        float bestScore = Float.MAX_VALUE;

        for(SpajaUser partner : randomSamplingOfUsers) {
            if(partner.getMasterPartitionId().intValue() == masterPartitionId.intValue()) {
                continue;
            }

            int mine   = manager.getPartitionById(partitionId).getNumReplicas();
            int theirs = manager.getPartitionById(partner.getPartitionId()).getNumReplicas();

            SwapChanges swapChanges = strategy.getSwapChanges(this, partner);

            int deltaMine   = swapChanges.getAddToP1().size() - swapChanges.getRemoveFromP1().size();
            int deltaTheirs = swapChanges.getAddToP2().size() - swapChanges.getRemoveFromP2().size();

            float oldScore = calcScore(mine,             theirs,               alpha);
            float newScore = calcScore(mine + deltaMine, theirs + deltaTheirs, alpha);

            if(newScore < bestScore && (newScore / t) < oldScore) {
                bestPartner = partner;
                bestScore = newScore;
            }
        }

        return bestPartner;
    }

    static float calcScore(int replicasInP1, int replicasInP2, float alpha) {
        return (float)(Math.pow(replicasInP1, alpha) + Math.pow(replicasInP2, alpha));
    }
}