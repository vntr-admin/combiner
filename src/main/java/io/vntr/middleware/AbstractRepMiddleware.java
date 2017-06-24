package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.manager.RepManager;
import io.vntr.utils.ProbabilityUtils;
import io.vntr.utils.TroveUtils;

import java.util.*;

import static io.vntr.utils.TroveUtils.convert;

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
    public void removeUser(Integer userId) {
        manager.removeUser(userId);
    }

    @Override
    public int addPartition() {
        return manager.addPartition();
    }

    @Override
    public void addPartition(Integer partitionId) {
        manager.addPartition(partitionId);
    }

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(RepUser user, TIntSet pidsToExclude) {
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
    public Set<Integer> getUserIds() {
        return convert(manager.getUids());
    }

    @Override
    public Set<Integer> getPartitionIds() {
        return convert(manager.getPids());
    }

    @Override
    public Integer getEdgeCut() {
        return manager.getEdgeCut();
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        return convert(manager.getPartitionToUserMap());
    }

    @Override
    public Integer getReplicationCount() {
        return manager.getReplicationCount();
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        return convert(manager.getFriendships());
    }

    @Override
    public double calculateAssortivity() {
        return ProbabilityUtils.calculateAssortivityCoefficient(getFriendships());
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToReplicasMap() {
        Map<Integer, Set<Integer>> m = new HashMap<>();
        for(int pid : getPartitionIds()) {
            m.put(pid, convert(manager.getReplicasOnPartition(pid)));
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

    TIntSet determineUsersWhoWillNeedAnAdditionalReplica(Integer partitionIdToBeRemoved) {
        TIntSet usersInNeedOfNewReplicas = new TIntHashSet();

        //First, determine which users will need more replicas once this partition is kaput
        for(TIntIterator iter = getManager().getMastersOnPartition(partitionIdToBeRemoved).iterator(); iter.hasNext(); ) {
            int userId = iter.next();
            RepUser user = getManager().getUserMaster(userId);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        for(TIntIterator iter = getManager().getReplicasOnPartition(partitionIdToBeRemoved).iterator(); iter.hasNext(); ) {
            int userId = iter.next();
            RepUser user = getManager().getUserMaster(userId);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
    }
}
