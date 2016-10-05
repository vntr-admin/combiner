package io.vntr.spaja;

import java.util.Set;

/**
 * Created by robertlindquist on 9/29/16.
 */
public class SwapChanges {
    private Integer pid1;
    private Integer pid2;
    private Set<Integer> addToP1;
    private Set<Integer> addToP2;
    private Set<Integer> removeToP1;
    private Set<Integer> removeToP2;

    public Integer getPid1() {
        return pid1;
    }

    public void setPid1(Integer pid1) {
        this.pid1 = pid1;
    }

    public Integer getPid2() {
        return pid2;
    }

    public void setPid2(Integer pid2) {
        this.pid2 = pid2;
    }

    public Set<Integer> getAddToP1() {
        return addToP1;
    }

    public void setAddToP1(Set<Integer> addToP1) {
        this.addToP1 = addToP1;
    }

    public Set<Integer> getAddToP2() {
        return addToP2;
    }

    public void setAddToP2(Set<Integer> addToP2) {
        this.addToP2 = addToP2;
    }

    public Set<Integer> getRemoveFromP1() {
        return removeToP1;
    }

    public void setRemoveFromP1(Set<Integer> removeToP1) {
        this.removeToP1 = removeToP1;
    }

    public Set<Integer> getRemoveFromP2() {
        return removeToP2;
    }

    public void setRemoveFromP2(Set<Integer> removeToP2) {
        this.removeToP2 = removeToP2;
    }
}
