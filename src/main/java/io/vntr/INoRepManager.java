package io.vntr;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/22/17.
 */
public interface INoRepManager {
    Integer addPartition();
    void addPartition(Integer pid);
    int addUser();
    void addUser(User user);
    void befriend(Integer id1, Integer id2);
    void checkValidity();
    Set<Integer> getPids();
    Integer getEdgeCut();
    Map<Integer, Set<Integer>> getFriendships();
    Integer getInitialPartitionId();
    long getMigrationTally();
    Integer getNumPartitions();
    Integer getNumUsers();
    Set<Integer> getPartition(Integer pid);
    Integer getPidForUser(Integer uid);
    Map<Integer, Set<Integer>> getPartitionToUsers();
    User getUser(Integer uid);
    Set<Integer> getUids();
    void increaseTally(int amount);
    void increaseTallyLogical(int amount);
    void moveUser(Integer uid, Integer newPid, boolean omitFromTally);
    void removePartition(Integer partitionId);
    void removeUser(Integer uid);
    void unfriend(Integer id1, Integer id2);
}
