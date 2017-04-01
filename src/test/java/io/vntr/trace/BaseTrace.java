package io.vntr.trace;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class BaseTrace {

    private Map<Integer, Set<Integer>> friendships;
    private Set<Integer> pids;
    private List<FullTraceAction> actions;

    public BaseTrace() {
    }

    public BaseTrace(Map<Integer, Set<Integer>> friendships, Set<Integer> pids, List<FullTraceAction> actions) {
        this.friendships = friendships;
        this.pids = pids;
        this.actions = actions;
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        return friendships;
    }

    public void setFriendships(Map<Integer, Set<Integer>> friendships) {
        this.friendships = friendships;
    }

    public Set<Integer> getPids() {
        return pids;
    }

    public void setPids(Set<Integer> pids) {
        this.pids = pids;
    }

    public List<FullTraceAction> getActions() {
        return actions;
    }

    public void setActions(List<FullTraceAction> actions) {
        this.actions = actions;
    }
}
