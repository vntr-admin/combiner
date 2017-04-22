package io.vntr;

import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 4/22/17.
 */
public interface IRepManager {
    Integer addPartition();
    void addPartition(Integer pid);
    void addReplica(RepUser user, Integer destPid);
    RepUser addReplicaNoUpdates(RepUser user, Integer destPid);
    int addUser();
    void addUser(RepUser user, Integer masterPartitionId);
    void addUser(User user);
    void befriend(RepUser smallerUser, RepUser largerUser);
    void checkValidity();
    Set<Integer> getPids();
    Set<Integer> getUids();
    Integer getEdgeCut();
    Map<Integer, Set<Integer>> getFriendships();
    long getMigrationTally();
    int getMinNumReplicas();
    int getNumUsers();
    Integer getPartitionIdWithFewestMasters();
    Set<Integer> getPartitionsToAddInitialReplicas(Integer masterPartitionId);
    Map<Integer, Set<Integer>> getPartitionToReplicasMap();
    Map<Integer, Set<Integer>> getPartitionToUserMap();
    Integer getRandomPartitionIdWhereThisUserIsNotPresent(RepUser user);
    Integer getReplicationCount();
    RepUser getUserMasterById(Integer id);
    void increaseTallyLogical(int amount);
    void increaseTally(int amount);
    void moveMasterAndInformReplicas(Integer uid, Integer fromPid, Integer toPid);
    void moveUser(RepUser user, Integer toPid, Set<Integer> replicateInDestinationPartition, Set<Integer> replicasToDeleteInSourcePartition);
    void promoteReplicaToMaster(Integer userId, Integer partitionId);
    void removePartition(Integer id);
    void removeReplica(RepUser user, Integer removalPartitionId);
    void removeUser(Integer userId);
    void unfriend(RepUser smallerUser, RepUser largerUser);
}
