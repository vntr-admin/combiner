package io.vntr.middleware;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.RepUser;
import io.vntr.User;
import io.vntr.befriend.SBefriender;
import io.vntr.manager.RepManager;
import io.vntr.utils.ProbabilityUtils;

import static io.vntr.utils.TroveUtils.getUToMasterMap;
import static io.vntr.utils.TroveUtils.singleton;

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
    public void unfriend(Integer smallerUid, Integer largerUid) {
        //When an edge between smallerUserId and largerUserId disappears,
        //the algorithm removes the replica of smallerUserId in the partition holding the master of node largerUserId
        //if no other node requires it, and vice-versa.
        //The algorithm checks whether there are more than K slave replicas before removing the node so that the desired redundancy level is maintained.

        RepUser smallerUser = getManager().getUserMaster(smallerUid);
        RepUser largerUser = getManager().getUserMaster(largerUid);

        if (!smallerUser.getBasePid().equals(largerUser.getBasePid())) {
            TIntIntMap uidToPidMap = getUToMasterMap(getPartitionToUserMap());
            TIntObjectMap<TIntSet> friendships = getFriendships();
            boolean smallerReplicaWasOnlyThereForLarger = SBefriender.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(largerUser, uidToPidMap, friendships).contains(smallerUid);
            boolean largerReplicaWasOnlyThereForSmaller = SBefriender.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(smallerUser, uidToPidMap, friendships).contains(largerUid);

            if (smallerReplicaWasOnlyThereForLarger && smallerUser.getReplicaPids().size() > getManager().getMinNumReplicas()) {
                getManager().removeReplica(smallerUser, largerUser.getBasePid());
            }
            if (largerReplicaWasOnlyThereForSmaller && largerUser.getReplicaPids().size() > getManager().getMinNumReplicas()) {
                getManager().removeReplica(largerUser, smallerUser.getBasePid());
            }
        }

        getManager().unfriend(smallerUser, largerUser);
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

    @Override
    public void removePartition(Integer pid) {
        //First, determine which users will be impacted by this action
        TIntSet affectedUsers = determineAffectedUsers(pid);

        //Second, determine the migration strategy
        TIntIntMap migrationStrategy = getMigrationStrategy(pid);

        //Third, promote replicas to masters as specified in the migration strategy
        for (Integer uid : migrationStrategy.keys()) {
            RepUser user = getManager().getUserMaster(uid);
            Integer newPid = migrationStrategy.get(uid);

            //If this is a simple water-filling one, there might not be a replica in the partition
            if (!user.getReplicaPids().contains(newPid)) {
                getManager().addReplica(user, newPid);
            }
            getManager().promoteReplicaToMaster(uid, newPid);
        }

        TIntSet usersToReplicate = getUsersToReplicate(affectedUsers, pid);

        //Fourth, add replicas as appropriate
        for(TIntIterator iter = usersToReplicate.iterator(); iter.hasNext(); ) {
            RepUser user = getManager().getUserMaster(iter.next());
            getManager().addReplica(user, getRandomPidWhereThisUserIsNotPresent(user, singleton(pid)));
        }

        //Fifth, remove references to replicas formerly on this partition
        for(TIntIterator iter = getManager().getReplicasOnPartition(pid).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            RepUser user = getManager().getUserMaster(uid);
            for(TIntIterator iter2 = user.getReplicaPids().iterator(); iter2.hasNext(); ) {
                getManager().getReplicaOnPartition(user.getId(), iter2.next()).removeReplicaPid(pid);
            }

            //Delete it from the master's replicaPids
            user.removeReplicaPid(pid);
        }

        //Finally, actually drop partition
        getManager().removePartition(pid);
    }

    TIntSet getUsersToReplicate(TIntSet uids, Integer pid) {
        TIntIntMap numReplicasAndMastersNotOnPartitionToBeRemoved = getCountOfReplicasAndMastersNotOnPartition(uids, pid);
        int minReplicas = getManager().getMinNumReplicas();
        TIntSet usersToReplicate = new TIntHashSet();
        for(TIntIterator iter = uids.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            if(numReplicasAndMastersNotOnPartitionToBeRemoved.get(uid) <= minReplicas) {
                usersToReplicate.add(uid);
            }
        }
        return usersToReplicate;
    }

    TIntIntMap getCountOfReplicasAndMastersNotOnPartition(TIntSet uids, Integer pid) {
        TIntIntMap counts = new TIntIntHashMap(uids.size()+1);
        for(TIntIterator iter = uids.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            int count = 0;
            RepUser user = getManager().getUserMaster(uid);
            count += user.getBasePid().equals(pid) ? 0 : 1;
            TIntSet replicas = user.getReplicaPids();
            int numReplicas = replicas.size();
            count += replicas.contains(pid) ? numReplicas - 1 : numReplicas;
            counts.put(uid, count);
        }
        return counts;
    }

    TIntSet determineAffectedUsers(Integer pidToBeRemoved) {
        TIntSet possibilities = new TIntHashSet(getManager().getMastersOnPartition(pidToBeRemoved));
        possibilities.addAll(getManager().getReplicasOnPartition(pidToBeRemoved));
        return possibilities;
    }

    abstract TIntIntMap getMigrationStrategy(int pid);
}
