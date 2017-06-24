package io.vntr;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Collection;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class RepUser extends User {

    private TIntSet replicaPids;

    public RepUser(Integer id) {
        super(id);
        this.replicaPids = new TIntHashSet();
    }

    public RepUser(Integer id, Integer basePid) {
        super(id, basePid);
        this.replicaPids = new TIntHashSet();
    }

    public TIntSet getReplicaPids() {
        return replicaPids;
    }

    public void addReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPids.add(replicaPartitionId);
    }

    public void removeReplicaPartitionId(Integer replicaPartitionId) {
        this.replicaPids.remove(replicaPartitionId);
    }

    public void addReplicaPartitionIds(TIntSet replicaPartitionIds) {
        this.replicaPids.addAll(replicaPartitionIds);
    }

    public RepUser dupe() {
        RepUser user = new RepUser(getId(), getBasePid());
        user.addReplicaPartitionIds(replicaPids);
        for(TIntIterator iter = getFriendIDs().iterator(); iter.hasNext(); ) {
            user.befriend(iter.next());
        }

        return user;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RepUser)) return false;
        if (!super.equals(o)) return false;

        RepUser repUser = (RepUser) o;

        return replicaPids != null ? replicaPids.equals(repUser.replicaPids) : repUser.replicaPids == null;
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
