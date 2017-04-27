package io.vntr.middleware;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/23/16.
 */
public interface IMiddlewareAnalyzer extends IMiddleware {
    Integer getNumberOfPartitions();
    Integer getNumberOfUsers();
    Integer getNumberOfFriendships();
    Set<Integer> getUserIds();
    Set<Integer> getPartitionIds();
    Integer getEdgeCut();
    Integer getReplicationCount();
    Long getMigrationTally();
    double calculateAssortivity();
    double calculateExpectedQueryDelay();
    void checkValidity();
    Map<Integer, Set<Integer>> getPartitionToUserMap();
    Map<Integer, Set<Integer>> getPartitionToReplicasMap();
    Map<Integer, Set<Integer>> getFriendships();
}
