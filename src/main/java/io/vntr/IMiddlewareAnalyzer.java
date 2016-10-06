package io.vntr;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/23/16.
 */
public interface IMiddlewareAnalyzer {
    Integer getNumberOfPartitions();
    Integer getNumberOfUsers();
    Integer getEdgeCut();
    Integer getReplicationCount();
    double calcualteAssortivity();
    Map<Integer, Set<Integer>> getPartitionToUserMap();
    Map<Integer, Set<Integer>> getFriendships();
}
