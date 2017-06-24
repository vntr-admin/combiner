package io.vntr.repartition;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;

/**
 * Created by robertlindquist on 4/26/17.
 */
public class RepResults {
    private final int numLogicalMoves;
    private final TIntIntMap uidToPidMap;
    private final TIntObjectMap<TIntSet> uidsToReplicaPids;

    public RepResults(int numLogicalMoves, TIntIntMap uidToPidMap, TIntObjectMap<TIntSet> uidsToReplicaPids) {
        this.numLogicalMoves = numLogicalMoves;
        this.uidToPidMap = uidToPidMap;
        this.uidsToReplicaPids = uidsToReplicaPids;
    }

    public int getNumLogicalMoves() {
        return numLogicalMoves;
    }

    public TIntIntMap getUidToPidMap() {
        return uidToPidMap;
    }

    public TIntObjectMap<TIntSet> getUidsToReplicaPids() {
        return uidsToReplicaPids;
    }
}
