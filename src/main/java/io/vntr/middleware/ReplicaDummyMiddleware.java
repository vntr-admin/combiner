package io.vntr.middleware;

import gnu.trove.map.TIntIntMap;
import io.vntr.RepUser;
import io.vntr.manager.RepManager;
import io.vntr.migration.SMigrator;

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
    public Long getMigrationTally() {
        return 0L; //Replica Dummy doesn't migrate users
    }

    @Override
    TIntIntMap getMigrationStrategy(int pid) {
        return SMigrator.getUserMigrationStrategy(pid, getFriendships(), getPartitionToUserMap(), getPartitionToReplicasMap(), false);
    }
}
