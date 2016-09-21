package io.vntr.jabeja;

import io.vntr.IMiddleware;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class JabejaMiddleware implements IMiddleware {

    private JabejaManager manager;

    public JabejaMiddleware(JabejaManager manager) {
        this.manager = manager;
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
    }

    @Override
    public void removeUser(Long userId) {
        manager.removeUser(userId);
    }

    @Override
    public void befriend(Long smallerUserId, Long largerUserId) {
        manager.befriend(smallerUserId, largerUserId);
        if(Math.random() > .9) {
            manager.repartition();
        }
    }

    @Override
    public void unfriend(Long smallerUserId, Long largerUserId) {
        manager.unfriend(smallerUserId, largerUserId);
    }

    @Override
    public void addPartition() {
        manager.addPartition();
    }

    @Override
    public void removePartition(Long partitionId) {
        Set<Long> partition = manager.getPartition(partitionId);
        List<Long> pids = new LinkedList<Long>(manager.getPartitionIds());
        manager.removePartition(partitionId);
        for(Long uid : partition) {
            Long newPid = ProbabilityUtils.getKDistinctValuesFromList(1, pids).iterator().next();
            manager.getUser(uid).setPid(newPid);
        }
    }

}
