package io.vntr;

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
    Collection<Integer> getUserIds();
    Collection<Integer> getPartitionIds();
    Integer getEdgeCut();
    Integer getReplicationCount();
    double calculateAssortivity();
    double calculateExpectedQueryDelay();
    Map<Integer, Set<Integer>> getPartitionToUserMap();
    Map<Integer, Set<Integer>> getPartitionToReplicaMap();
    Map<Integer, Set<Integer>> getFriendships();
}
