package io.vntr.hermes;

import io.vntr.IMiddleware;
import io.vntr.User;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class HermesMiddleware  implements IMiddleware {
    private HermesManager manager;
    public HermesMiddleware(double gamma) {
        manager = new HermesManager(gamma);
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
        manager.repartition();
    }

    @Override
    public void removeUser(Long userId) {
        manager.removeUser(userId);
    }

    @Override
    public void befriend(Long smallerUserId, Long largerUserId) {
        manager.befriend(smallerUserId, largerUserId);
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
        //TODO: do this
    }
}
