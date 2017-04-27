package io.vntr.repartition;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/26/17.
 */
public class RepResults {
    private final int numLogicalMoves;
    private final Map<Integer, Integer> uidToPidMap;
    private final Map<Integer, Set<Integer>> uidsToReplicaPids;

    public RepResults(int numLogicalMoves, Map<Integer, Integer> uidToPidMap, Map<Integer, Set<Integer>> uidsToReplicaPids) {
        this.numLogicalMoves = numLogicalMoves;
        this.uidToPidMap = uidToPidMap;
        this.uidsToReplicaPids = uidsToReplicaPids;
    }

    public int getNumLogicalMoves() {
        return numLogicalMoves;
    }

    public Map<Integer, Integer> getUidToPidMap() {
        return uidToPidMap;
    }

    public Map<Integer, Set<Integer>> getUidsToReplicaPids() {
        return uidsToReplicaPids;
    }
}
