package io.vntr.spar;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import io.vntr.User;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

public class SparUser extends User {
    private Integer masterPid;
    private Set<Integer> replicaPids;

    public SparUser(Integer id) {
        super(id);
        replicaPids = new HashSet<>();
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
    public SparUser clone() {
        SparUser user = new SparUser(getId());
        user.setMasterPid(masterPid);
        user.addReplicaPartitionIds(replicaPids);
        for (Integer friendId : getFriendIDs()) {
            user.befriend(friendId);
        }

        return user;
    }

    @Override
    public String toString() {
        return super.toString() + "|masterP:" + masterPid + "|Reps:" + replicaPids.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparUser)) return false;

        SparUser that = (SparUser) o;

        return     safeEquals(masterPid,           that.masterPid)
                && safeEquals(this.getId(),        that.getId())
                && safeEquals(replicaPids,         that.getReplicaPids())
                && safeEquals(this.getFriendIDs(), that.getFriendIDs());
    }

    @Override
    public int hashCode() {
        int result = safeHashCode(masterPid);
        result = 31 * result + safeHashCode(getId());
        result = 31 * result + safeHashCode(replicaPids);
        result = 31 * result + safeHashCode(getFriendIDs());
        return result;
    }
}