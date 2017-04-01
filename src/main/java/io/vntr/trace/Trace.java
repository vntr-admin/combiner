package io.vntr.trace;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class Trace {
    private Map<Integer, Set<Integer>> friendships;
    private List<TraceAction> actions;
    private Map<Integer, Set<Integer>> partitions;
    private Map<Integer, Set<Integer>> replicas;

    public Trace() {
    }

    public Trace(Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        this.partitions = partitions;
        this.replicas = replicas;
    }

    public Trace(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas, List<TraceAction> actions) {
        this.friendships = friendships;
        this.partitions = partitions;
        this.actions = actions;
        this.replicas = replicas;
    }

    public Map<Integer, Set<Integer>> getFriendships() {
        return friendships;
    }

    public void setFriendships(Map<Integer, Set<Integer>> friendships) {
        this.friendships = friendships;
    }

    public Set<Integer> getPids() {
        return partitions.keySet();
    }

    public List<TraceAction> getActions() {
        return actions;
    }

    public void setActions(List<TraceAction> actions) {
        this.actions = actions;
    }

    public Map<Integer, Set<Integer>> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<Integer, Set<Integer>> partitions) {
        this.partitions = partitions;
    }

    public Map<Integer, Set<Integer>> getReplicas() {
        return replicas;
    }

    public void setReplicas(Map<Integer, Set<Integer>> replicas) {
        this.replicas = replicas;
    }
}
