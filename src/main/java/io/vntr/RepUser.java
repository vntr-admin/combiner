package io.vntr;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class RepUser extends User {

    private TShortSet replicaPids;

    public RepUser(short id, short basePid) {
        super(id, basePid);
        this.replicaPids = new TShortHashSet();
    }

    public TShortSet getReplicaPids() {
        return replicaPids;
    }

    public void addReplicaPids(short replicaPid) {
        this.replicaPids.add(replicaPid);
    }

    public void removeReplicaPid(short replicaPid) {
        this.replicaPids.remove(replicaPid);
    }

    public void addReplicaPids(TShortSet replicaPid) {
        this.replicaPids.addAll(replicaPid);
    }

    public RepUser dupe() {
        RepUser user = new RepUser(getId(), getBasePid());
        user.addReplicaPids(replicaPids);
        for(TShortIterator iter = getFriendIDs().iterator(); iter.hasNext(); ) {
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
