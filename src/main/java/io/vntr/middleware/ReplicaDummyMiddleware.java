package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.set.TIntSet;
import io.vntr.RepUser;
import io.vntr.manager.RepManager;

import static io.vntr.migration.RepWaterFillingMigrator.getUserMigrationStrategy;
import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class ReplicaDummyMiddleware extends AbstractRepMiddleware {

    public ReplicaDummyMiddleware(RepManager manager) {
        super(manager);
    }

    @Override
    public void befriend(Integer smallerUid, Integer largerUid) {
        RepUser smallerUser = getManager().getUserMaster(smallerUid);
        RepUser largerUser = getManager().getUserMaster(largerUid);
        getManager().befriend(smallerUser, largerUser);

        int smallerPid = smallerUser.getBasePid();
        int largerPid = largerUser.getBasePid();
        if(smallerPid != largerPid) {
            if(!getManager().getReplicasOnPartition(largerPid).contains(smallerUid)) {
                getManager().addReplica(smallerUser, largerPid);
            }
            if(!getManager().getReplicasOnPartition(smallerPid).contains(largerUid)) {
                getManager().addReplica(largerUser, smallerPid);
            }
        }
    }

    @Override
    public void unfriend(Integer smallerUid, Integer largerUid) {
        RepUser smallerUser = getManager().getUserMaster(smallerUid);
        RepUser largerUser = getManager().getUserMaster(largerUid);
        getManager().unfriend(smallerUser, largerUser);
    }

    @Override
    public void removePartition(Integer pid) {
        //First, determine which users will need more replicas once this partition is kaput
        TIntSet usersInNeedOfNewReplicas = determineUsersWhoWillNeedAnAdditionalReplica(pid);
        
        //Second, determine the migration strategy
        TIntIntMap migrationStrategy = getUserMigrationStrategy(pid, getManager().getFriendships(), getManager().getPartitionToUserMap(), getManager().getPartitionToReplicasMap());

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer uid : migrationStrategy.keys()) {
            RepUser user = getManager().getUserMaster(uid);
            Integer newPid = migrationStrategy.get(uid);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPid)) {
                getManager().addReplica(user, newPid);
                usersInNeedOfNewReplicas.remove(uid);
            }
            getManager().promoteReplicaToMaster(uid, newPid);
        }

        //Fourth, add replicas as appropriate
        for(TIntIterator iter = usersInNeedOfNewReplicas.iterator(); iter.hasNext(); ) {
            RepUser user = getManager().getUserMaster(iter.next());
            getManager().addReplica(user, getRandomPidWhereThisUserIsNotPresent(user, singleton(pid)));
        }

        //Fifth, remove references to replicas formerly on this partition
        for(TIntIterator iter = getManager().getReplicasOnPartition(pid).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            for(TIntIterator iter2 = user.getReplicaPids().iterator(); iter2.hasNext(); ) {
                getManager().getReplicaOnPartition(user.getId(), iter2.next()).removeReplicaPid(pid);
            }

            //Delete it from the master's replicaPids
            user.removeReplicaPid(pid);
        }

        //Finally, actually drop partition
        getManager().removePartition(pid);
    }

    @Override
    public Long getMigrationTally() {
        return 0L; //Replica Dummy doesn't migrate users
    }

}
