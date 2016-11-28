package io.vntr.dummy;

import io.vntr.User;

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
}
