package io.vntr.spaja;

import java.util.Set;

/**
 * Created by robertlindquist on 9/29/16.
 */
public class SwapChanges {
    private Long pid1;
    private Long pid2;
    private Set<Long> addToP1;
    private Set<Long> addToP2;
    private Set<Long> removeToP1;
    private Set<Long> removeToP2;

    public Long getPid1() {
        return pid1;
    }

    public void setPid1(Long pid1) {
        this.pid1 = pid1;
    }

    public Long getPid2() {
        return pid2;
    }

    public void setPid2(Long pid2) {
        this.pid2 = pid2;
    }

    public Set<Long> getAddToP1() {
        return addToP1;
    }

    public void setAddToP1(Set<Long> addToP1) {
        this.addToP1 = addToP1;
    }

    public Set<Long> getAddToP2() {
        return addToP2;
    }

    public void setAddToP2(Set<Long> addToP2) {
        this.addToP2 = addToP2;
    }

    public Set<Long> getRemoveFromP1() {
        return removeToP1;
    }

    public void setRemoveFromP1(Set<Long> removeToP1) {
        this.removeToP1 = removeToP1;
    }

    public Set<Long> getRemoveFromP2() {
        return removeToP2;
    }

    public void setRemoveFromP2(Set<Long> removeToP2) {
        this.removeToP2 = removeToP2;
    }
}
