package io.vntr.middleware;

import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.manager.RepManager;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    Integer getRandomPartitionIdWhereThisUserIsNotPresent(RepUser user, Collection<Integer> pidsToExclude) {
        Predicate<Integer> predicate = x -> !user.getBasePid().equals(x) && !user.getReplicaPids().contains(x) && !pidsToExclude.contains(x);
        List<Integer> list = manager.getPids().stream().filter(predicate).collect(Collectors.toList());
        return list.get((int) (list.size() * Math.random()));
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
        for(Set<Integer> friends : getFriendships().values()) {
            numFriendships += friends.size();
        }
        return numFriendships / 2;
    }

    @Override
    public Set<Integer> getUserIds() {
        return manager.getUids();
    }

    @Override
    public Set<Integer> getPartitionIds() {
        return manager.getPids();
    }

    @Override
    public Integer getEdgeCut() {
        return manager.getEdgeCut();
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToUserMap() {
        return manager.getPartitionToUserMap();
    }

    @Override
    public Integer getReplicationCount() {
        return manager.getReplicationCount();
    }

    @Override
    public Map<Integer, Set<Integer>> getFriendships() {
        return manager.getFriendships();
    }

    @Override
    public double calculateAssortivity() {
        return ProbabilityUtils.calculateAssortivityCoefficient(getFriendships());
    }

    @Override
    public Map<Integer, Set<Integer>> getPartitionToReplicasMap() {
        Map<Integer, Set<Integer>> m = new HashMap<>();
        for(int pid : getPartitionIds()) {
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

    Set<Integer> determineUsersWhoWillNeedAnAdditionalReplica(Integer partitionIdToBeRemoved) {
        Set<Integer> usersInNeedOfNewReplicas = new HashSet<>();

        //First, determine which users will need more replicas once this partition is kaput
        for (Integer userId : getManager().getMastersOnPartition(partitionIdToBeRemoved)) {
            RepUser user = getManager().getUserMaster(userId);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        for (Integer userId : getManager().getReplicasOnPartition(partitionIdToBeRemoved)) {
            RepUser user = getManager().getUserMaster(userId);
            if (user.getReplicaPids().size() <= getManager().getMinNumReplicas()) {
                usersInNeedOfNewReplicas.add(userId);
            }
        }

        return usersInNeedOfNewReplicas;
    }
}
