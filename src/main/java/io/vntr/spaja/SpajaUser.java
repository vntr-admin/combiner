package io.vntr.spaja;

import io.vntr.User;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

/**
 * Created by robertlindquist on 9/28/16.
 * Should be done
 */
public class SpajaUser extends User {
    private float alpha;
    private Integer masterPid;
    private Set<Integer> replicaPids;
    private SpajaManager manager; //TODO: this is sloppy; replace its usage with passing in the necessary info
    private int k;

    public SpajaUser(Integer id, float alpha, int k, SpajaManager manager) {
        super(id);
        replicaPids = new HashSet<>();
        this.alpha = alpha;
        this.k = k;
        this.manager = manager;
    }

    public Integer getMasterPid() {
        return masterPid;
    }

    public void setMasterPid(Integer masterPid) {
        this.masterPid = masterPid;
    }

    public void addReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPids.add(replicaPartitionId);
    }

    public void removeReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPids.remove(replicaPartitionId);
    }

    public void addReplicaPartitionIds(Collection<Integer> replicaPartitionIds) {
        this.replicaPids.addAll(replicaPartitionIds);
    }

    public Set<Integer> getReplicaPids() {
        return replicaPids;
    }

    @Override
    public SpajaUser clone() {
        SpajaUser user = new SpajaUser(getId(), alpha, k, manager);
        user.setMasterPid(masterPid);
        user.addReplicaPartitionIds(replicaPids);
        for (Integer friendId : getFriendIDs()) {
            user.befriend(friendId);
        }

        return user;
    }

    @Override
    public String toString() {
        return super.toString() + "|M:" + masterPid + "|R:" + replicaPids.toString();
    }

    public SpajaUser findPartner(Collection<SpajaUser> randomSamplingOfUsers, float t, SpajaBefriendingStrategy strategy) {
        SpajaUser bestPartner = null;
        float bestScore = Float.MAX_VALUE;

        for(SpajaUser partner : randomSamplingOfUsers) {
            if(partner.getMasterPid().intValue() == masterPid.intValue()) {
                continue;
            }

            int mine   = manager.getPartitionById(masterPid).getNumReplicas();
            int theirs = manager.getPartitionById(partner.getMasterPid()).getNumReplicas();

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SpajaUser)) return false;

        SpajaUser that = (SpajaUser) o;

        return this.k == that.k
                && (Float.compare(this.alpha,      that.alpha) == 0)
                && safeEquals(this.masterPid,      that.masterPid)
                && safeEquals(this.getId(),        that.getId())
                && safeEquals(this.replicaPids,    that.replicaPids)
                && safeEquals(this.getFriendIDs(), that.getFriendIDs());
    }

    @Override
    public int hashCode() {
        int result = k;
        result = 31 * result + (alpha != +0.0f ? Float.floatToIntBits(alpha) : 0);
        result = 31 * result + safeHashCode(masterPid);
        result = 31 * result + safeHashCode(getId());
        result = 31 * result + safeHashCode(replicaPids);
        result = 31 * result + safeHashCode(getFriendIDs());
        return result;
    }
}