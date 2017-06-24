package io.vntr.repartition;

import gnu.trove.map.TIntIntMap;

/**
 * Created by robertlindquist on 4/22/17.
 */
public class NoRepResults {
    private final TIntIntMap uidsToPids;
    private final int logicalMoves;

    public NoRepResults(TIntIntMap uidsToPids, int logicalMoves) {
        this.uidsToPids = uidsToPids;
        this.logicalMoves = logicalMoves;
    }

    public TIntIntMap getUidsToPids() {
        return uidsToPids;
    }

    public int getLogicalMoves() {
        return logicalMoves;
    }

}
