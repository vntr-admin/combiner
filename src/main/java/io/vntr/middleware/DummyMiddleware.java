package io.vntr.middleware;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.set.TShortSet;
import io.vntr.manager.NoRepManager;

import static io.vntr.utils.TroveUtils.getRandomElement;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class DummyMiddleware extends AbstractNoRepMiddleware {
    private NoRepManager manager;

    public DummyMiddleware(NoRepManager manager) {
        super(manager);
        this.manager = manager;
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
    public long getMigrationTally() {
        return 0L; //No migration in dummy
    }

    @Override
    public void broadcastDowntime() {
        //There's nothing to do here
    }

}
