package io.vntr.trace;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;

import java.util.List;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class BaseTrace {

    private TShortObjectMap<TShortSet> friendships;
    private TShortSet pids;
    private List<FullTraceAction> actions;

    public BaseTrace() {
    }

    public BaseTrace(TShortObjectMap<TShortSet> friendships, TShortSet pids, List<FullTraceAction> actions) {
        this.friendships = friendships;
        this.pids = pids;
        this.actions = actions;
    }

    public TShortObjectMap<TShortSet> getFriendships() {
        return friendships;
    }

    public void setFriendships(TShortObjectMap<TShortSet> friendships) {
        this.friendships = friendships;
    }

    public TShortSet getPids() {
        return pids;
    }

    public void setPids(TShortSet pids) {
        this.pids = pids;
    }

    public List<FullTraceAction> getActions() {
        return actions;
    }

    public void setActions(List<FullTraceAction> actions) {
        this.actions = actions;
    }
}
