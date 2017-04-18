package io.vntr.metis;

import io.vntr.User;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

/**
 * Created by robertlindquist on 12/5/16.
 */
public class MetisUser extends User {

    private Integer pid;

    public MetisUser(Integer id, Integer initialPid) {
        super(id);
        this.pid = initialPid;
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    public String toString() {
        return super.toString() + "|P:" + pid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetisUser)) return false;

        MetisUser that = (MetisUser) o;

        return     safeEquals(this.pid,            that.pid)
                && safeEquals(this.getId(),        that.getId())
                && safeEquals(this.getFriendIDs(), that.getFriendIDs());
    }

    @Override
    public int hashCode() {
        int result = safeHashCode(pid);
        result = 31 * result + safeHashCode(getId());
        result = 31 * result + safeHashCode(getFriendIDs());
        return result;
    }
}
