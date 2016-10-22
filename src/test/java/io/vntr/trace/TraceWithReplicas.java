package io.vntr.trace;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class TraceWithReplicas extends TraceWithPartitions {

    private Map<Integer, Set<Integer>> replicas;

    public TraceWithReplicas() {
    }

    public TraceWithReplicas(Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        super(partitions);
        this.replicas = replicas;
    }

    public TraceWithReplicas(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas, List<FullTraceAction> actions) {
        super(friendships, partitions, actions);
        this.replicas = replicas;
    }

    public Map<Integer, Set<Integer>> getReplicas() {
        return replicas;
    }

    public void setReplicas(Map<Integer, Set<Integer>> replicas) {
        this.replicas = replicas;
    }
}
