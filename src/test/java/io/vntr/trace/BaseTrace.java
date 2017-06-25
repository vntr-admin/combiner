package io.vntr.trace;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;

import java.util.List;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class BaseTrace {

    private TIntObjectMap<TIntSet> friendships;
    private TIntSet pids;
    private List<FullTraceAction> actions;

    public BaseTrace() {
    }

    public BaseTrace(TIntObjectMap<TIntSet> friendships, TIntSet pids, List<FullTraceAction> actions) {
        this.friendships = friendships;
        this.pids = pids;
        this.actions = actions;
    }

    public TIntObjectMap<TIntSet> getFriendships() {
        return friendships;
    }

    public void setFriendships(TIntObjectMap<TIntSet> friendships) {
        this.friendships = friendships;
    }

    public TIntSet getPids() {
        return pids;
    }

    public void setPids(TIntSet pids) {
        this.pids = pids;
    }

    public List<FullTraceAction> getActions() {
        return actions;
    }

    public void setActions(List<FullTraceAction> actions) {
        this.actions = actions;
    }
}
