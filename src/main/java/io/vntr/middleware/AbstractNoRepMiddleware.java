package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.utils.ProbabilityUtils;

import static io.vntr.migration.NoRepWaterFillingMigrator.migrateOffPartition;

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
    public int addUser() {
        return manager.addUser();
    }

    @Override
    public void addUser(User user) {
        manager.addUser(user);
    }

    @Override
    public void removeUser(Integer uid) {
        manager.removeUser(uid);
    }

    @Override
    public void befriend(Integer smallerUid, Integer largerUid) {
        manager.befriend(smallerUid, largerUid);
    }

    @Override
    public void unfriend(Integer smallerUid, Integer largerUid) {
        manager.unfriend(smallerUid, largerUid);
    }

    @Override
    public int addPartition() {
        return manager.addPartition();
    }

    @Override
    public void addPartition(Integer pid) {
        manager.addPartition(pid);
    }

    @Override
    public void removePartition(Integer pid) {
        TIntIntMap strategy = migrateOffPartition(pid, getPartitionToUserMap());
        getManager().removePartition(pid);
        for(int uid : strategy.keys()) {
            int newPid = strategy.get(uid);
            getManager().moveUser(uid, newPid, true);
        }
    }

    @Override
    public void broadcastDowntime() {
        //There's nothing to do here
    }

    @Override
    public Integer getNumberOfPartitions() {
        return manager.getNumPartitions();
    }

    @Override
    public Integer getNumberOfUsers() {
        return manager.getNumUsers();
    }

    @Override
    public Integer getNumberOfFriendships() {
        int numFriendships=0;
        for(TIntSet friends : manager.getFriendships().valueCollection()) {
            numFriendships += friends.size();
        }
        return numFriendships / 2;
    }

    @Override
    public TIntSet getUserIds() {
        return manager.getUids();
    }

    @Override
    public TIntSet getPids() {
        return manager.getPids();
    }

    @Override
    public Integer getEdgeCut() {
        return manager.getEdgeCut();
    }


    @Override
    public TIntObjectMap<TIntSet> getPartitionToUserMap() {
        return manager.getPartitionToUsers();
    }

    @Override
    public TIntObjectMap<TIntSet> getFriendships() {
        return manager.getFriendships();
    }

    @Override
    public double calculateAssortivity() {
        return ProbabilityUtils.calculateAssortivityCoefficient(getFriendships());
    }

    @Override
    public TIntObjectMap<TIntSet> getPartitionToReplicasMap() {
        TIntObjectMap<TIntSet> m = new TIntObjectHashMap<>(getNumberOfPartitions()+1);
        for(TIntIterator iter = manager.getPids().iterator(); iter.hasNext(); ) {
            m.put(iter.next(), new TIntHashSet());
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
    public Long getMigrationTally() {
        return manager.getMigrationTally();
    }

    @Override
    public Integer getReplicationCount() {
        return 0; //No-Rep doesn't replicate
    }


}
