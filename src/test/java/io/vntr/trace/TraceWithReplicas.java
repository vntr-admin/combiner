package io.vntr.trace;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;

import java.util.List;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class TraceWithReplicas extends TraceWithPartitions {

    private TShortObjectMap<TShortSet> replicas;

    public TraceWithReplicas() {
    }

    public TraceWithReplicas(TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas) {
        super(partitions);
        this.replicas = replicas;
    }

    public TraceWithReplicas(TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas, List<FullTraceAction> actions) {
        super(friendships, partitions, actions);
        this.replicas = replicas;
    }

    public TShortObjectMap<TShortSet> getReplicas() {
        return replicas;
    }

    public void setReplicas(TShortObjectMap<TShortSet> replicas) {
        this.replicas = replicas;
    }
}
