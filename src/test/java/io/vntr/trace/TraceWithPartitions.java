package io.vntr.trace;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;

import java.util.List;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class TraceWithPartitions extends BaseTrace {
    private TIntObjectMap<TIntSet> partitions;

    public TraceWithPartitions() {
    }

    public TraceWithPartitions(TIntObjectMap<TIntSet> partitions) {
        this.partitions = partitions;
    }

    public TraceWithPartitions(TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> partitions, List<FullTraceAction> actions) {
        super(friendships, null, actions);
        this.partitions = partitions;
    }

    @Override
    public TIntSet getPids() {
        return partitions.keySet();
    }

    @Override
    public void setPids(TIntSet pids) {
        throw new RuntimeException("setPids not allowed in TraceWithPartitions");
    }

    public TIntObjectMap<TIntSet> getPartitions() {
        return partitions;
    }

    public void setPartitions(TIntObjectMap<TIntSet> partitions) {
        this.partitions = partitions;
    }
}
