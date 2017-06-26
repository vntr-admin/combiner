package io.vntr.repartition;

import gnu.trove.map.TShortShortMap;

/**
 * Created by robertlindquist on 4/22/17.
 */
public class NoRepResults {
    private final TShortShortMap uidsToPids;
    private final int logicalMoves;

    public NoRepResults(TShortShortMap uidsToPids, int logicalMoves) {
        this.uidsToPids = uidsToPids;
        this.logicalMoves = logicalMoves;
    }

    public TShortShortMap getUidsToPids() {
        return uidsToPids;
    }

    public int getLogicalMoves() {
        return logicalMoves;
    }

}
