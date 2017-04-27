package io.vntr.middleware;

import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

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
    public void removePartition(Integer partitionId) {
        Set<Integer> partition = manager.getPartition(partitionId);
        manager.removePartition(partitionId);
        for(Integer uid : partition) {
            Integer newPid = ProbabilityUtils.getRandomElement(manager.getPids());
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
