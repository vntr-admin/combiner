package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
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
    public int addPartition() {
        return manager.addPartition();
    }

    @Override
    public void addPartition(Integer pid) {
        manager.addPartition(pid);
    }

    Integer getRandomPidWhereThisUserIsNotPresent(RepUser user, TIntSet pidsToExclude) {
        TIntSet potentialReplicaLocations = new TIntHashSet(manager.getPids());
        potentialReplicaLocations.removeAll(pidsToExclude);
        potentialReplicaLocations.remove(user.getBasePid());
        potentialReplicaLocations.removeAll(user.getReplicaPids());
        int[] array = potentialReplicaLocations.toArray();
        return array[(int) (array.length * Math.random())];
    }

    @Override
    public Integer getNumberOfPartitions() {
        return manager.getPids().size();
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
        return manager.getPartitionToUserMap();
    }

    @Override
    public Integer getReplicationCount() {
        return manager.getReplicationCount();
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
            int pid = iter.next();
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
    public Long getMigrationTally() {
        return getManager().getMigrationTally();
    }

    @Override
    public void broadcastDowntime() {
        //SPAR ignores downtime
    }

    TIntSet determineUsersWhoWillNeedAnAdditionalReplica(Integer pidToBeRemoved) {
        TIntSet usersInNeedOfNewReplicas = new TIntHashSet();

        //First, determine which users will need more replicas once this partition is kaput
        for(TIntIterator iter = getManager().getMastersOnPartition(pidToBeRemoved).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(uid);
            }
        }

        for(TIntIterator iter = getManager().getReplicasOnPartition(pidToBeRemoved).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(uid);
            }
        }

        return usersInNeedOfNewReplicas;
    }
}
