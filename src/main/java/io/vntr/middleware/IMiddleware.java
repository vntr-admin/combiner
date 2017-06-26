package io.vntr.middleware;

import io.vntr.User;

public interface IMiddleware {

    short addUser();

    void addUser(User user);

    void removeUser(short uid);

    void befriend(short smallerUid, short largerUid);

    void unfriend(short smallerUid, short largerUid);

    short addPartition();

    void addPartition(short pid);

    void removePartition(short pid);

    void broadcastDowntime();
}