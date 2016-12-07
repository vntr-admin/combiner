package io.vntr.metis;

import io.vntr.User;

import java.util.Collection;

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
}
