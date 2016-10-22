package io.vntr.trace;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class TraceWithPartitions extends Trace {
    private Map<Integer, Set<Integer>> partitions;

    public TraceWithPartitions() {
    }

    public TraceWithPartitions(Map<Integer, Set<Integer>> partitions) {
        this.partitions = partitions;
    }

    public TraceWithPartitions(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, List<FullTraceAction> actions) {
        super(friendships, null, actions);
        this.partitions = partitions;
    }

    @Override
    public Set<Integer> getPids() {
        return partitions.keySet();
    }

    @Override
    public void setPids(Set<Integer> pids) {
        throw new RuntimeException("setPids not allowed in TraceWithPartitions");
    }

    public Map<Integer, Set<Integer>> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<Integer, Set<Integer>> partitions) {
        this.partitions = partitions;
    }
}
