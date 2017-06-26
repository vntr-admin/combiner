package io.vntr.middleware;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.set.TShortSet;
import io.vntr.RepUser;
import io.vntr.manager.RepManager;

import static io.vntr.migration.SMigrator.getUserMigrationStrategy;
import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class ReplicaDummyMiddleware extends AbstractRepMiddleware {

    public ReplicaDummyMiddleware(RepManager manager) {
        super(manager);
    }

    @Override
    public void befriend(short smallerUid, short largerUid) {
        RepUser smallerUser = getManager().getUserMaster(smallerUid);
        RepUser largerUser = getManager().getUserMaster(largerUid);
        getManager().befriend(smallerUser, largerUser);

        short smallerPid = smallerUser.getBasePid();
        short largerPid = largerUser.getBasePid();
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
    public void unfriend(short smallerUid, short largerUid) {
        RepUser smallerUser = getManager().getUserMaster(smallerUid);
        RepUser largerUser = getManager().getUserMaster(largerUid);
        getManager().unfriend(smallerUser, largerUser);
    }

    @Override
    public void removePartition(short pid) {
        //First, determine which users will need more replicas once this partition is kaput
        TShortSet usersInNeedOfNewReplicas = determineUsersWhoWillNeedAnAdditionalReplica(pid);
        
        //Second, determine the migration strategy
        TShortShortMap migrationStrategy = getUserMigrationStrategy(pid, getManager().getFriendships(), getManager().getPartitionToUserMap(), getManager().getPartitionToReplicasMap(), false);

        //Third, promote replicas to masters as specified in the migration strategy
        for (short uid : migrationStrategy.keys()) {
            RepUser user = getManager().getUserMaster(uid);
            short newPid = migrationStrategy.get(uid);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPid)) {
                getManager().addReplica(user, newPid);
                usersInNeedOfNewReplicas.remove(uid);
            }
            getManager().promoteReplicaToMaster(uid, newPid);
        }

        //Fourth, add replicas as appropriate
        for(TShortIterator iter = usersInNeedOfNewReplicas.iterator(); iter.hasNext(); ) {
            RepUser user = getManager().getUserMaster(iter.next());
            getManager().addReplica(user, getRandomPidWhereThisUserIsNotPresent(user, singleton(pid)));
        }

        //Fifth, remove references to replicas formerly on this partition
        for(TShortIterator iter = getManager().getReplicasOnPartition(pid).iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            for(TShortIterator iter2 = user.getReplicaPids().iterator(); iter2.hasNext(); ) {
                getManager().getReplicaOnPartition(user.getId(), iter2.next()).removeReplicaPid(pid);
            }

            //Delete it from the master's replicaPids
            user.removeReplicaPid(pid);
        }

        //Finally, actually drop partition
        getManager().removePartition(pid);
    }

    @Override
    public long getMigrationTally() {
        return 0L; //Replica Dummy doesn't migrate users
    }

}
