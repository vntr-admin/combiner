package io.vntr.trace;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;

import java.util.List;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class Trace {
    private TShortObjectMap<TShortSet> friendships;
    private List<TraceAction> actions;
    private TShortObjectMap<TShortSet> partitions;
    private TShortObjectMap<TShortSet> replicas;

    public Trace() {
    }

    public TShortObjectMap<TShortSet> getFriendships() {
        return friendships;
    }

    public void setFriendships(TShortObjectMap<TShortSet> friendships) {
        this.friendships = friendships;
    }

    public List<TraceAction> getActions() {
        return actions;
    }

    public void setActions(List<TraceAction> actions) {
        this.actions = actions;
    }

    public TShortObjectMap<TShortSet> getPartitions() {
        return partitions;
    }

    public void setPartitions(TShortObjectMap<TShortSet> partitions) {
        this.partitions = partitions;
    }

    public TShortObjectMap<TShortSet> getReplicas() {
        return replicas;
    }

    public void setReplicas(TShortObjectMap<TShortSet> replicas) {
        this.replicas = replicas;
    }
}
