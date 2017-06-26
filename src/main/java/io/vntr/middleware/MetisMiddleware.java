package io.vntr.middleware;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.set.TShortSet;
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
    public void befriend(short smallerUid, short largerUid) {
        super.befriend(smallerUid, largerUid);
        if(Math.random() > .9) {
            repartition();
        }
    }

    @Override
    public void removePartition(short pid) {
        TShortSet partition = manager.getPartition(pid);
        manager.removePartition(pid);
        for(TShortIterator iter = partition.iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            short newPid = getRandomElement(manager.getPids());
            manager.moveUser(uid, newPid, true);
        }
    }

    @Override
    public void broadcastDowntime() {
        repartition();
    }

    void repartition() {
        TShortShortMap newPartitioning = MetisRepartitioner.partition(gpmetisLocation, gpmetisTempdir, getManager().getFriendships(), getManager().getPartitionToUsers().keySet());
        for(short uid : newPartitioning.keys()) {
            short newPid = newPartitioning.get(uid);
            if(newPid != manager.getUser(uid).getBasePid()) {
                manager.moveUser(uid, newPid, false);
            }
        }
    }


}
