package io.vntr.dummy;

import io.vntr.User;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class DummyUser extends User {
    private Integer pid;

    public DummyUser(Integer id, Integer pid) {
        super(id);
        this.pid = pid;
    }

    public Integer getPid() {
        return pid;
    }

    public void setPid(Integer pid) {
        this.pid = pid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DummyUser)) return false;

        DummyUser that = (DummyUser) o;

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
