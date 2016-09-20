package io.vntr.jabeja;

import io.vntr.IMiddleware;
import io.vntr.User;

/**
 * Created by robertlindquist on 9/19/16.
 */
public class JabejaMiddleware implements IMiddleware {

    @Override
    public void addUser(User user) {
        //TODO: do this
    }

    @Override
    public void removeUser(Long userId) {
        //TODO: do this
    }

    @Override
    public void befriend(Long smallerUserId, Long largerUserId) {
        //TODO: do this
    }

    @Override
    public void unfriend(Long smallerUserId, Long largerUserId) {
        //TODO: do this
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
