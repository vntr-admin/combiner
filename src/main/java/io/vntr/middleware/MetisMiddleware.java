package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.set.TIntSet;
import io.vntr.manager.NoRepManager;
import io.vntr.repartition.MetisRepartitioner;

import static io.vntr.utils.TroveUtils.getRandomElement;

/**
 * Created by robertlindquist on 12/5/16.
 */
public class MetisMiddleware extends AbstractNoRepMiddleware {

    private final String gpmetisLocation;
    private final String gpmetisTempdir;

    private NoRepManager manager;

    public MetisMiddleware(String gpmetisLocation, String gpmetisTempdir, NoRepManager manager) {
        super(manager);
        this.gpmetisLocation = gpmetisLocation;
        this.gpmetisTempdir = gpmetisTempdir;
        this.manager = manager;
    }

    @Override
    public void befriend(Integer smallerUid, Integer largerUid) {
        super.befriend(smallerUid, largerUid);
        if(Math.random() > .9) {
            repartition();
        }
    }

    @Override
    public void removePartition(Integer pid) {
        TIntSet partition = manager.getPartition(pid);
        manager.removePartition(pid);
        for(TIntIterator iter = partition.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            Integer newPid = getRandomElement(manager.getPids());
            manager.moveUser(uid, newPid, true);
        }
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    void repartition() {
        TIntIntMap newPartitioning = MetisRepartitioner.partition(gpmetisLocation, gpmetisTempdir, getManager().getFriendships(), getManager().getPartitionToUsers().keySet());
        for(int uid : newPartitioning.keys()) {
            int newPid = newPartitioning.get(uid);
            if(newPid != manager.getUser(uid).getBasePid()) {
                manager.moveUser(uid, newPid, false);
            }
        }
    }


}
