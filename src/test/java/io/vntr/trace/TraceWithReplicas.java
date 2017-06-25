package io.vntr.trace;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;

import java.util.List;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class TraceWithReplicas extends TraceWithPartitions {

    private TIntObjectMap<TIntSet> replicas;

    public TraceWithReplicas() {
    }

    public TraceWithReplicas(TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas) {
        super(partitions);
        this.replicas = replicas;
    }

    public TraceWithReplicas(TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas, List<FullTraceAction> actions) {
        super(friendships, partitions, actions);
        this.replicas = replicas;
    }

    public TIntObjectMap<TIntSet> getReplicas() {
        return replicas;
    }

    public void setReplicas(TIntObjectMap<TIntSet> replicas) {
        this.replicas = replicas;
    }
}
