package io.vntr.middleware;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.set.TIntSet;

/**
 * Created by robertlindquist on 9/23/16.
 */
public interface IMiddlewareAnalyzer extends IMiddleware {
    Integer getNumberOfPartitions();
    Integer getNumberOfUsers();
    Integer getNumberOfFriendships();
    TIntSet getUserIds();
    TIntSet getPartitionIds();
    Integer getEdgeCut();
    Integer getReplicationCount();
    Long getMigrationTally();
    double calculateAssortivity();
    double calculateExpectedQueryDelay();
    void checkValidity();
    TIntObjectMap<TIntSet> getPartitionToUserMap();
    TIntObjectMap<TIntSet> getPartitionToReplicasMap();
    TIntObjectMap<TIntSet> getFriendships();
}
