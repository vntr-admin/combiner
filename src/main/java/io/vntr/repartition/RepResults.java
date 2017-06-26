package io.vntr.repartition;

import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;

/**
 * Created by robertlindquist on 4/26/17.
 */
public class RepResults {
    private final int numLogicalMoves;
    private final TShortShortMap uidToPidMap;
    private final TShortObjectMap<TShortSet> uidsToReplicaPids;

    public RepResults(int numLogicalMoves, TShortShortMap uidToPidMap, TShortObjectMap<TShortSet> uidsToReplicaPids) {
        this.numLogicalMoves = numLogicalMoves;
        this.uidToPidMap = uidToPidMap;
        this.uidsToReplicaPids = uidsToReplicaPids;
    }

    public int getNumLogicalMoves() {
        return numLogicalMoves;
    }

    public TShortShortMap getUidToPidMap() {
        return uidToPidMap;
    }

    public TShortObjectMap<TShortSet> getUidsToReplicaPids() {
        return uidsToReplicaPids;
    }
}
