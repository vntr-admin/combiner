package io.vntr.middleware;

import io.vntr.RepUser;
import io.vntr.manager.RepManager;

import java.util.*;

import static io.vntr.migration.DummyMigrator.getUserMigrationStrategy;
import static io.vntr.utils.Utils.getUToReplicasMap;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class ReplicaDummyMiddleware extends AbstractRepMiddleware {

    public ReplicaDummyMiddleware(RepManager manager) {
        super(manager);
    }

    @Override
    public void befriend(Integer smallerUserId, Integer largerUserId) {
        RepUser smallerUser = getManager().getUserMaster(smallerUserId);
        RepUser largerUser = getManager().getUserMaster(largerUserId);
        getManager().befriend(smallerUser, largerUser);

        int smallerPid = smallerUser.getBasePid();
        int largerPid = largerUser.getBasePid();
        if(smallerPid != largerPid) {
            if(!getManager().getReplicasOnPartition(largerPid).contains(smallerUserId)) {
                getManager().addReplica(smallerUser, largerPid);
            }
            if(!getManager().getReplicasOnPartition(smallerPid).contains(largerUserId)) {
                getManager().addReplica(largerUser, smallerPid);
            }
        }
    }

    @Override
    public void unfriend(Integer smallerUserId, Integer largerUserId) {
        RepUser smallerUser = getManager().getUserMaster(smallerUserId);
        RepUser largerUser = getManager().getUserMaster(largerUserId);
        getManager().unfriend(smallerUser, largerUser);
    }

    @Override
    public void removePartition(Integer partitionId) {
        //First, determine which users will need more replicas once this partition is kaput
        Set<Integer> usersInNeedOfNewReplicas = determineUsersWhoWillNeedAnAdditionalReplica(partitionId);
        
        //Second, determine the migration strategy
        Map<Integer, Set<Integer>> uidToReplicasMap = getUToReplicasMap(getPartitionToReplicaMap(), getFriendships().keySet());
        Map<Integer, Integer> migrationStrategy = getUserMigrationStrategy(partitionId, getManager().getPartitionToUserMap(), uidToReplicasMap);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keySet()) {
            RepUser user = getManager().getUserMaster(userId);
            Integer newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPartitionId)) {
                getManager().addReplica(user, newPartitionId);
                usersInNeedOfNewReplicas.remove(userId);
            }
            getManager().promoteReplicaToMaster(userId, migrationStrategy.get(userId));
        }

        //Fourth, add replicas as appropriate
        for (Integer userId : usersInNeedOfNewReplicas) {
            RepUser user = getManager().getUserMaster(userId);
            int newPid = getRandomPartitionIdWhereThisUserIsNotPresent(user, Collections.singletonList(partitionId));
            getManager().addReplica(user, newPid);
        }

        //Fifth, remove references to replicas formerly on this partition
        for(Integer uid : getManager().getReplicasOnPartition(partitionId)) {
            RepUser user = getManager().getUserMaster(uid);
            for (Integer currentReplicaPartitionId : user.getReplicaPids()) {
                getManager().getReplicaOnPartition(user.getId(), currentReplicaPartitionId).removeReplicaPartitionId(partitionId);
            }

            //Delete it from the master's replicaPartitionIds
            user.removeReplicaPartitionId(partitionId);
        }

        //Finally, actually drop partition
        getManager().removePartition(partitionId);
    }

    Set<Integer> determineUsersWhoWillNeedAnAdditionalReplica(Integer partitionIdToBeRemoved) {
        Set<Integer> usersInNeedOfNewReplicas = new HashSet<>();

        //First, determine which users will need more replicas once this partition is kaput
        for (Integer userId : getManager().getMastersOnPartition(partitionIdToBeRemoved)) {
            RepUser user = getManager().getUserMaster(userId);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        for (Integer userId : getManager().getReplicasOnPartition(partitionIdToBeRemoved)) {
            RepUser user = getManager().getUserMaster(userId);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
    }

    @Override
    public Long getMigrationTally() {
        return 0L; //Replica Dummy doesn't migrate users
    }

    @Override
    public void broadcastDowntime() {
        //ignores downtime
    }

}
