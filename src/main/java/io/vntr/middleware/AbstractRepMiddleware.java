package io.vntr.middleware;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.manager.RepManager;
import io.vntr.utils.ProbabilityUtils;

/**
 * Created by robertlindquist on 4/27/17.
 */
public abstract class AbstractRepMiddleware implements IMiddlewareAnalyzer {

    private RepManager manager;

    public AbstractRepMiddleware(RepManager manager) {
        this.manager = manager;
    }

    protected RepManager getManager() {
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
    public short addPartition() {
        return manager.addPartition();
    }

    @Override
    public void addPartition(short pid) {
        manager.addPartition(pid);
    }

    short getRandomPidWhereThisUserIsNotPresent(RepUser user, TShortSet pidsToExclude) {
        TShortSet potentialReplicaLocations = new TShortHashSet(manager.getPids());
        potentialReplicaLocations.removeAll(pidsToExclude);
        potentialReplicaLocations.remove(user.getBasePid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        short[] array = potentialReplicaLocations.toArray();
        return array[(int) (array.length * Math.random())];
    }

    @Override
    public short getNumberOfPartitions() {
        return (short) manager.getPids().size();
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
        return manager.getPartitionToUserMap();
    }

    @Override
    public int getReplicationCount() {
        return manager.getReplicationCount();
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
            short pid = iter.next();
            m.put(pid, manager.getReplicasOnPartition(pid));
        }
        return m;
    }

    @Override
    public String toString() {
        return manager.toString();
    }

    @Override
    public double calculateExpectedQueryDelay() {
        return 0; //Replica systems are strictly-local by design
    }

    @Override
    public void checkValidity() {
        manager.checkValidity();
    }

    @Override
    public long getMigrationTally() {
        return getManager().getMigrationTally();
    }

    @Override
    public void broadcastDowntime() {
        //SPAR ignores downtime
    }

    TShortSet determineUsersWhoWillNeedAnAdditionalReplica(short pidToBeRemoved) {
        TShortSet usersInNeedOfNewReplicas = new TShortHashSet();

        //First, determine which users will need more replicas once this partition is kaput
        for(TShortIterator iter = getManager().getMastersOnPartition(pidToBeRemoved).iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(uid);
            }
        }

        for(TShortIterator iter = getManager().getReplicasOnPartition(pidToBeRemoved).iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(uid);
            }
        }

        return usersInNeedOfNewReplicas;
    }
}
