package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
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
    public Long getMigrationTally() {
        return 0L; //No migration in dummy
    }

    @Override
    public void broadcastDowntime() {
        //There's nothing to do here
    }

}
