package io.vntr.trace;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;

import java.util.List;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class TraceWithPartitions extends BaseTrace {
    private TShortObjectMap<TShortSet> partitions;

    public TraceWithPartitions() {
    }

    public TraceWithPartitions(TShortObjectMap<TShortSet> partitions) {
        this.partitions = partitions;
    }

    public TraceWithPartitions(TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> partitions, List<FullTraceAction> actions) {
        super(friendships, null, actions);
        this.partitions = partitions;
    }

    @Override
    public TShortSet getPids() {
        return partitions.keySet();
    }

    @Override
    public void setPids(TShortSet pids) {
        throw new RuntimeException("setPids not allowed in TraceWithPartitions");
    }

    public TShortObjectMap<TShortSet> getPartitions() {
        return partitions;
    }

    public void setPartitions(TShortObjectMap<TShortSet> partitions) {
        this.partitions = partitions;
    }
}
