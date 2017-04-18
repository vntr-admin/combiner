package io.vntr.j2;

import io.vntr.User;

import java.util.Collection;

import static io.vntr.Utils.safeEquals;
import static io.vntr.Utils.safeHashCode;

/**
 * Created by robertlindquist on 4/12/17.
 */
public class J2User extends User{
    private Integer pid;

    public J2User(Integer id, Integer initialPid) {
        super(id);
        this.pid = initialPid;
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
        if (!(o instanceof J2User)) return false;

        J2User j2User = (J2User) o;

        return safeEquals(this.getId(), j2User.getId())
                && safeEquals(this.pid, j2User.pid)
                && safeEquals(this.getFriendIDs(), j2User.getFriendIDs());
    }

    @Override
    public int hashCode() {
        int result = safeHashCode(pid);
        result = 31 * result + safeHashCode(getId());
        return 31 * result + safeHashCode(getFriendIDs());
    }
}
