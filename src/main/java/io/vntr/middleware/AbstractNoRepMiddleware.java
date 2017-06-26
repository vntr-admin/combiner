package io.vntr.middleware;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.utils.ProbabilityUtils;

/**
 * Created by robertlindquist on 4/27/17.
 */
public abstract class AbstractNoRepMiddleware implements IMiddlewareAnalyzer {
    private NoRepManager manager;

    public AbstractNoRepMiddleware(NoRepManager manager) {
        this.manager = manager;
    }

    protected NoRepManager getManager() {
        return manager;
    }

    @Override
    public short addUser() {
        return manager.addUser();
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
    }

    @Override
    public void removeUser(short uid) {
        manager.removeUser(uid);
    }

    @Override
    public void befriend(short smallerUid, short largerUid) {
        manager.befriend(smallerUid, largerUid);
    }

    @Override
    public void unfriend(short smallerUid, short largerUid) {
        manager.unfriend(smallerUid, largerUid);
    }

    @Override
    public short addPartition() {
        return manager.addPartition();
    }

    @Override
    public void addPartition(short pid) {
        manager.addPartition(pid);
    }

    @Override
    public short getNumberOfPartitions() {
        return manager.getNumPartitions();
    }

    @Override
    public short getNumberOfUsers() {
        return manager.getNumUsers();
    }

    @Override
    public int getNumberOfFriendships() {
        int numFriendships=0;
        for(TShortSet friends : manager.getFriendships().valueCollection()) {
            numFriendships += friends.size();
        }
        return numFriendships / 2;
    }

    @Override
    public TShortSet getUserIds() {
        return manager.getUids();
    }

    @Override
    public TShortSet getPids() {
        return manager.getPids();
    }

    @Override
    public int getEdgeCut() {
        return manager.getEdgeCut();
    }


    @Override
    public TShortObjectMap<TShortSet> getPartitionToUserMap() {
        return manager.getPartitionToUsers();
    }

    @Override
    public TShortObjectMap<TShortSet> getFriendships() {
        return manager.getFriendships();
    }

    @Override
    public double calculateAssortivity() {
        return ProbabilityUtils.calculateAssortivityCoefficient(getFriendships());
    }

    @Override
    public TShortObjectMap<TShortSet> getPartitionToReplicasMap() {
        TShortObjectMap<TShortSet> m = new TShortObjectHashMap<>(getNumberOfPartitions()+1);
        for(TShortIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            m.put(iter.next(), new TShortHashSet());
        }
        return m;
    }

    @Override
    public String toString() {
        return manager.toString();
    }

    @Override
    public double calculateExpectedQueryDelay() {
        return ProbabilityUtils.calculateExpectedQueryDelay(getFriendships(), getPartitionToUserMap());
    }

    @Override
    public void checkValidity() {
        manager.checkValidity();
    }

    @Override
    public long getMigrationTally() {
        return manager.getMigrationTally();
    }

    @Override
    public int getReplicationCount() {
        return 0; //No-Rep doesn't replicate
    }


}
