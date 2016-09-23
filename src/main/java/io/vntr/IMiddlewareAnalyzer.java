package io.vntr;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 9/23/16.
 */
public interface IMiddlewareAnalyzer {
    Long getNumberOfPartitions();
    Long getNumberOfUsers();
    Long getEdgeCut();
    Map<Long, Set<Long>> getPartitionToUserMap();
}
