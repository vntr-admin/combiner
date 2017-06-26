package io.vntr.middleware;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;

/**
 * Created by robertlindquist on 9/23/16.
 */
public interface IMiddlewareAnalyzer extends IMiddleware {
    short getNumberOfPartitions();
    short getNumberOfUsers();
    int getNumberOfFriendships();
    TShortSet getUserIds();
    TShortSet getPids();
    int getEdgeCut();
    int getReplicationCount();
    long getMigrationTally();
    double calculateAssortivity();
    double calculateExpectedQueryDelay();
    void checkValidity();
    TShortObjectMap<TShortSet> getPartitionToUserMap();
    TShortObjectMap<TShortSet> getPartitionToReplicasMap();
    TShortObjectMap<TShortSet> getFriendships();
}
