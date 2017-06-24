package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.set.TIntSet;
import io.vntr.RepUser;
import io.vntr.manager.RepManager;

import static io.vntr.migration.SMigrator.getUserMigrationStrategy;
import static io.vntr.utils.TroveUtils.singleton;

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
        TIntSet usersInNeedOfNewReplicas = determineUsersWhoWillNeedAnAdditionalReplica(partitionId);
        
        //Second, determine the migration strategy
        TIntIntMap migrationStrategy = getUserMigrationStrategy(partitionId, getManager().getFriendships(), getManager().getPartitionToUserMap(), getManager().getPartitionToReplicasMap(), false);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer userId : migrationStrategy.keys()) {
            RepUser user = getManager().getUserMaster(userId);
            Integer newPartitionId = migrationStrategy.get(userId);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPartitionId)) {
                getManager().addReplica(user, newPartitionId);
                usersInNeedOfNewReplicas.remove(userId);
            }
            getManager().promoteReplicaToMaster(userId, newPartitionId);
        }

        //Fourth, add replicas as appropriate
        for(TIntIterator iter = usersInNeedOfNewReplicas.iterator(); iter.hasNext(); ) {
            RepUser user = getManager().getUserMaster(iter.next());
            getManager().addReplica(user, getRandomPartitionIdWhereThisUserIsNotPresent(user, singleton(partitionId)));
        }

        //Fifth, remove references to replicas formerly on this partition
        for(TIntIterator iter = getManager().getReplicasOnPartition(partitionId).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            for(TIntIterator iter2 = user.getReplicaPids().iterator(); iter2.hasNext(); ) {
                getManager().getReplicaOnPartition(user.getId(), iter2.next()).removeReplicaPartitionId(partitionId);
            }

            //Delete it from the master's replicaPartitionIds
            user.removeReplicaPartitionId(partitionId);
        }

        //Finally, actually drop partition
        getManager().removePartition(partitionId);
    }

    @Override
    public Long getMigrationTally() {
        return 0L; //Replica Dummy doesn't migrate users
    }

}
