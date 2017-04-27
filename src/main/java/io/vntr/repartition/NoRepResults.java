package io.vntr.repartition;

import java.util.Map;

/**
 * Created by robertlindquist on 4/22/17.
 */
public class NoRepResults {
    private final Map<Integer, Integer> uidsToPids;
    private final int logicalMoves;

    public NoRepResults(Map<Integer, Integer> uidsToPids, int logicalMoves) {
        this.uidsToPids = uidsToPids;
        this.logicalMoves = logicalMoves;
    }

    public Map<Integer, Integer> getUidsToPids() {
        return uidsToPids;
    }

    public int getLogicalMoves() {
        return logicalMoves;
    }

}