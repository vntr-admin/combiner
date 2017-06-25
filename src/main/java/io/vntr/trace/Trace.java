package io.vntr.trace;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;

import java.util.List;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class Trace {
    private TIntObjectMap<TIntSet> friendships;
    private List<TraceAction> actions;
    private TIntObjectMap<TIntSet> partitions;
    private TIntObjectMap<TIntSet> replicas;

    public Trace() {
    }

    public TIntObjectMap<TIntSet> getFriendships() {
        return friendships;
    }

    public void setFriendships(TIntObjectMap<TIntSet> friendships) {
        this.friendships = friendships;
    }

    public List<TraceAction> getActions() {
        return actions;
    }

    public void setActions(List<TraceAction> actions) {
        this.actions = actions;
    }

    public TIntObjectMap<TIntSet> getPartitions() {
        return partitions;
    }

    public void setPartitions(TIntObjectMap<TIntSet> partitions) {
        this.partitions = partitions;
    }

    public TIntObjectMap<TIntSet> getReplicas() {
        return replicas;
    }

    public void setReplicas(TIntObjectMap<TIntSet> replicas) {
        this.replicas = replicas;
    }
}
