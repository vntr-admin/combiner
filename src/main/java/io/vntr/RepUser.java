package io.vntr;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class RepUser extends User {

    private TIntSet replicaPids;

    public RepUser(int id, int basePid) {
        super(id, basePid);
        this.replicaPids = new TIntHashSet();
    }

    public TIntSet getReplicaPids() {
        return replicaPids;
    }

    public void addReplicaPids(Integer replicaPid) {
        this.replicaPids.add(replicaPid);
    }

    public void removeReplicaPid(Integer replicaPid) {
        this.replicaPids.remove(replicaPid);
    }

    public void addReplicaPids(TIntSet replicaPid) {
        this.replicaPids.addAll(replicaPid);
    }

    public RepUser dupe() {
        RepUser user = new RepUser(getId(), getBasePid());
        user.addReplicaPids(replicaPids);
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
