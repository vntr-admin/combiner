package io.vntr;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static io.vntr.Utils.safeEquals;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class RepUser extends User {

    private Set<Integer> replicaPids;

    public RepUser(Integer id) {
        super(id);
        this.replicaPids = new HashSet<>();
    }

    public RepUser(Integer id, Integer basePid) {
        super(id, basePid);
        this.replicaPids = new HashSet<>();
    }

    public Set<Integer> getReplicaPids() {
        return replicaPids;
    }

    public void setReplicaPids(Set<Integer> replicaPids) {
        this.replicaPids = replicaPids;
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

    public RepUser dupe() {
        RepUser user = new RepUser(getId(), getBasePid());
        user.addReplicaPartitionIds(replicaPids);
        for (Integer friendId : getFriendIDs()) {
            user.befriend(friendId);
        }

        return user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RepUser)) return false;
        if (!super.equals(o)) return false;

        RepUser repUser = (RepUser) o;

        return safeEquals(this.replicaPids, repUser.replicaPids);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (replicaPids != null ? replicaPids.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return super.toString() + "|Reps:" + replicaPids.toString();
    }
}
