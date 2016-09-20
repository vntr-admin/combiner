package io.vntr.jabeja;

import io.vntr.IMiddleware;
import io.vntr.User;

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
        //TODO: do this
    }

    @Override
    public void removePartition(Long partitionId) {
        //TODO: do this
    }

}
