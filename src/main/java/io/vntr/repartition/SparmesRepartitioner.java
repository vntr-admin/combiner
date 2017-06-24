package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/22/17.
 */
public class SparmesRepartitioner {

    public static RepResults repartition(int k, int maxIterations, float gamma, int minNumReplicas, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas, TIntObjectMap<TIntSet> friendships) {
        int moves = 0;
        State state = State.init(minNumReplicas, gamma, partitions, replicas, friendships);

        for(int i=0; i<maxIterations; i++) {
            int movesBeforeIteration = moves;
            moves += performStage(true, k, state);
            state.updateLogicalUsers();

            moves += performStage(false, k, state);
            state.updateLogicalUsers();

            if(moves == movesBeforeIteration) {
                break;
            }
        }

        TIntIntMap uidsToPids = getUToMasterMap(state.getLogicalPartitions());
        TIntObjectMap<TIntSet> uidsToReplicaPids = getUToReplicasMap(state.getLogicalReplicaPartitions(), friendships.keySet());

        return new RepResults(moves, uidsToPids, uidsToReplicaPids);
    }

    static int performStage(boolean firstStage, int k, State state) {
        Set<Target> targets = new HashSet<>();

        for(int pid : state.getLogicalPartitions().keys()) {
            targets.addAll(getPartitionCandidates(pid, firstStage, k, state));
        }

        TIntObjectMap<TIntSet> uidToReplicaPids = getUToReplicasMap(state.getLogicalReplicaPartitions(), state.getLogicalUsers().keySet());
        for(Target target : targets) {
            migrateLogically(target, state, uidToReplicaPids);
        }

        return targets.size();
    }

    static Set<Target> getPartitionCandidates(int pid, boolean firstIteration, int k, State state) {
        NavigableSet<Target> candidates = new TreeSet<>();
        for(TIntIterator iter = state.getLogicalPartitions().get(pid).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            Target target = state.getLogicalUsers().get(uid).getTarget(firstIteration);
            if(target.pid != null) {
                candidates.add(target);
            }
        }

        Set<Target> topKCandidates = new HashSet<>();
        int i=0;
        for(Iterator<Target> iter = candidates.descendingIterator(); iter.hasNext() && i++<k; ) {
            topKCandidates.add(iter.next());
        }

        return topKCandidates;
    }

    static void migrateLogically(Target target, State state, TIntObjectMap<TIntSet> uidToReplicaPids) {
        TIntObjectMap<TIntSet> logicalPids = state.getLogicalPartitions();
        TIntObjectMap<TIntSet> logicalReplicaPids = state.getLogicalReplicaPartitions();

        TIntSet oldMasters = logicalPids.get(target.oldPid);
        TIntSet newMasters = logicalPids.get(target.pid);
        TIntSet oldReplicas = logicalReplicaPids.get(target.oldPid);
        TIntSet newReplicas = logicalReplicaPids.get(target.pid);

        LogicalUser user = state.getLogicalUsers().get(target.uid);

        //Add the actual user
        oldMasters.remove(target.uid);
        newMasters.add(target.uid);


        //Add replicas as necessary

        //First, replicate user in old partition if necessary
        if(!disjoint(user.getFriendIds(), oldMasters)) {
            oldReplicas.add(target.uid);
        }

        //Second, replicate friends in new partition if they aren't there already
        for(TIntIterator iter = user.getFriendIds().iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            if(!newMasters.contains(friendId) && !newReplicas.contains(friendId)) {
                newReplicas.add(friendId);
            }
        }


        //Remove replicas as allowed

        //First, remove user replica from new partition if one exists
        newReplicas.remove(target.uid); //doesn't cause a problem if it wasn't already there

        //Second, if we've violated k-constraints, choose another partition at random and replicate this user there
        TIntSet replicaPids = uidToReplicaPids.get(target.uid);
        if(replicaPids.size() < user.getMinNumReplicas()) {
            TIntSet potentialReplicaLocations = new TIntHashSet(logicalReplicaPids.keySet());
            potentialReplicaLocations.remove(target.pid);
            potentialReplicaLocations.removeAll(replicaPids);
            int newReplicaPid = getRandomElement(potentialReplicaLocations);
            logicalReplicaPids.get(newReplicaPid).add(target.uid);
        }

        //Third, remove friends replicas from old partition if they weren't being used for any other reason and don't violate k-replication
        TIntSet friendReplicasToRemoveFromOldPartition = new TIntHashSet(user.getFriendIds());
        friendReplicasToRemoveFromOldPartition.retainAll(oldReplicas);

        for(TIntIterator iter = friendReplicasToRemoveFromOldPartition.iterator(); iter.hasNext(); ) {
            int friendId = iter.next();
            TIntSet friendsOfFriend = state.getLogicalUsers().get(friendId).getFriendIds();
            if(disjoint(friendsOfFriend, oldMasters)) {
                int numFriendReplicas = uidToReplicaPids.get(friendId).size();
                if(numFriendReplicas > user.getMinNumReplicas()) {
                    oldReplicas.remove(friendId);
                }
            }
        }
    }

    static class State {
        private int minNumReplicas;
        private float gamma;
        private TIntObjectMap<TIntSet> friendships;

        private TIntObjectMap<LogicalUser> logicalUsers;
        private TIntObjectMap<TIntSet> logicalPartitions;
        private TIntObjectMap<TIntSet> logicalReplicaPartitions;

        public State() {
        }

        public TIntObjectMap<LogicalUser> getLogicalUsers() {
            return logicalUsers;
        }

        public void setLogicalUsers(TIntObjectMap<LogicalUser> logicalUsers) {
            this.logicalUsers = logicalUsers;
        }

        public TIntObjectMap<TIntSet> getLogicalPartitions() {
            return logicalPartitions;
        }

        public void setLogicalPartitions(TIntObjectMap<TIntSet> logicalPartitions) {
            this.logicalPartitions = logicalPartitions;
        }

        public TIntObjectMap<TIntSet> getLogicalReplicaPartitions() {
            return logicalReplicaPartitions;
        }

        public void setLogicalReplicaPartitions(TIntObjectMap<TIntSet> logicalReplicaPartitions) {
            this.logicalReplicaPartitions = logicalReplicaPartitions;
        }

        static State init(int minNumReplicas, float gamma, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas, TIntObjectMap<TIntSet> friendships) {
            State state = new State();
            state.minNumReplicas = minNumReplicas;
            state.gamma = gamma;
            state.friendships = friendships;

            TIntObjectMap<TIntSet> logicalPartitions = new TIntObjectHashMap<>(partitions.size()+1);
            for(int pid : partitions.keys()) {
                logicalPartitions.put(pid, new TIntHashSet(partitions.get(pid)));
            }
            state.setLogicalPartitions(logicalPartitions);

            TIntObjectMap<TIntSet> logicalReplicas = new TIntObjectHashMap<>(partitions.size()+1);
            for(int pid : replicas.keys()) {
                logicalReplicas.put(pid, new TIntHashSet(replicas.get(pid)));
            }
            state.setLogicalReplicaPartitions(logicalReplicas);

            state.updateLogicalUsers();

            return state;
        }

        void updateLogicalUsers() {
            setLogicalUsers(initLogicalUsers(minNumReplicas, gamma, logicalPartitions, logicalReplicaPartitions, friendships));
        }

        static TIntObjectMap<LogicalUser> initLogicalUsers(int minNumReplicas, float gamma, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> replicas, TIntObjectMap<TIntSet> friendships) {
            TIntObjectMap<LogicalUser> logicalUsers = new TIntObjectHashMap<>(friendships.size()+1);

            int maxPid = max(partitions.keySet());
            int maxUid = max(friendships.keySet());

            TIntIntMap uidToPidMap = getUToMasterMap(partitions);
            TIntObjectMap<TIntSet> uidToReplicaMap = getUToReplicasMap(replicas, friendships.keySet());
            int[] numDeletionCandidates = getNumDeletionCandidates(maxUid, minNumReplicas, friendships, replicas, uidToPidMap, uidToReplicaMap);
            TIntObjectMap<TIntIntMap> uidToNumFriendsToAddInEachPartition = getUidToNumFriendsToAddInEachPartition(friendships, uidToReplicaMap, uidToPidMap, partitions.keySet(), maxPid);

            TIntIntMap pToWeight = getUserCounts(partitions);
            int totalWeight = 0;
            for(int key : pToWeight.keys()) {
                totalWeight += pToWeight.get(key);
            }

            for(int uid : friendships.keys()) {
                TIntSet friendIds = new TIntHashSet(friendships.get(uid));
                TIntIntMap pToFriendCount = getPToFriendCount(friendIds, uidToPidMap, partitions.keySet(), maxPid);

                int numFriendReplicasToDeleteInSourcePartition = numDeletionCandidates[uid];
                boolean replicateInSourcePartition = !disjoint(friendIds, partitions.get(uidToPidMap.get(uid)));

                LogicalUser user = new LogicalUser(
                        uid,
                        uidToPidMap.get(uid),
                        gamma,
                        friendIds,
                        pToFriendCount,
                        new TIntIntHashMap(pToWeight),
                        new TIntHashSet(uidToReplicaMap.get(uid)),
                        uidToNumFriendsToAddInEachPartition.get(uid),
                        numFriendReplicasToDeleteInSourcePartition,
                        replicateInSourcePartition,
                        totalWeight,
                        minNumReplicas
                );

                logicalUsers.put(uid, user);
            }

            return logicalUsers;
        }

        static int[] getNumDeletionCandidates(int maxUid, int minNumReplicas, TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> replicas, TIntIntMap uidToPidMap, TIntObjectMap<TIntSet> uidToReplicasMap) {
            int[] numDeletionCandidates = new int[maxUid+1];

            for(int pid : replicas.keys()) {
middle:         for(TIntIterator iter = replicas.get(pid).iterator(); iter.hasNext(); ) {
                    int replicaId = iter.next();
                    Integer friendOnPartition = null;

                    for(TIntIterator iter2 = friendships.get(replicaId).iterator(); iter2.hasNext(); ) {
                        int friendId = iter2.next();
                        if(uidToPidMap.get(friendId) == pid) {
                            if(friendOnPartition != null) {
                                continue middle;
                            }
                            friendOnPartition = friendId;
                        }
                    }
                    if(friendOnPartition != null && uidToReplicasMap.get(replicaId).size() > minNumReplicas) {
                        numDeletionCandidates[friendOnPartition]++;
                    }
                }
            }

            return numDeletionCandidates;
        }

        static TIntObjectMap<TIntIntMap> getUidToNumFriendsToAddInEachPartition(TIntObjectMap<TIntSet> friendships, TIntObjectMap<TIntSet> uidToReplicaMap, TIntIntMap uidToPidMap, TIntSet pids, int maxPid) {
            TIntObjectMap<TIntIntMap> uidToNumFriendsToAddInEachPartition = new TIntObjectHashMap<>(friendships.size()+1);

            for(int uid : friendships.keys()) {
                //you have to add a replica of each friend who isn't present in the partition
                TIntSet friendIds = friendships.get(uid);
                int[] friendsToAdd = new int[maxPid+1];
                Arrays.fill(friendsToAdd, friendIds.size());

                for(TIntIterator iter = friendIds.iterator(); iter.hasNext(); ) {
                    int friendId = iter.next();
                    for(TIntIterator iter2 = uidToReplicaMap.get(friendId).iterator(); iter2.hasNext(); ) {
                        int friendLocation = iter2.next();
                        friendsToAdd[friendLocation]--;
                    }
                    friendsToAdd[uidToPidMap.get(friendId)]--;
                }

                int pid = uidToPidMap.get(uid);
                TIntIntMap numFriendsToAddInEachPartition = new TIntIntHashMap(pids.size()+1);
                for(TIntIterator iter = pids.iterator(); iter.hasNext(); ) {
                    int pid1 = iter.next();
                    if(pid1 != pid) {
                        numFriendsToAddInEachPartition.put(pid1, friendsToAdd[pid1]);
                    }
                }
                uidToNumFriendsToAddInEachPartition.put(uid, numFriendsToAddInEachPartition);
            }
            return uidToNumFriendsToAddInEachPartition;
        }

        static TIntIntMap getPToFriendCount(TIntSet friendIds, TIntIntMap uidToPidMap, TIntSet pids, int maxPid) {
            int[] friendCounts = new int[maxPid+1];
            for(TIntIterator iter = friendIds.iterator(); iter.hasNext(); ) {
                int friendId = iter.next();
                friendCounts[uidToPidMap.get(friendId)]++;
            }
            TIntIntMap pToFriendCount = new TIntIntHashMap(pids.size()+1);
            for(TIntIterator iter = pids.iterator(); iter.hasNext(); ) {
                int pid = iter.next();
                pToFriendCount.put(pid, friendCounts[pid]);
            }
            return pToFriendCount;
        }

        void checkStateValidity() {
            for(LogicalUser user : this.getLogicalUsers().valueCollection()) {
                if(user.replicaLocations.size() < this.minNumReplicas) {
                    throw new RuntimeException("User " + user.id + " has " + this.minNumReplicas + "-replication problem.");
                }
                for(int pid : this.getLogicalPartitions().keys()) {
                    boolean userHasMasterHere = this.getLogicalPartitions().get(pid).contains(user.id);
                    boolean userIsReplicatedHere = this.getLogicalReplicaPartitions().get(pid).contains(user.id);

                    boolean userThinksItsMasterIsHere = user.pid == pid;
                    boolean userThinksItIsReplicatedHere = user.replicaLocations.contains(pid);

                    if(userHasMasterHere != userThinksItsMasterIsHere) {
                        throw new RuntimeException("User " + user.id + " has a master problem in " + pid);
                    }
                    if(userIsReplicatedHere != userThinksItIsReplicatedHere) {
                        throw new RuntimeException("User " + user.id + " has a replica problem in " + pid);
                    }
                }
                for(TIntIterator iter = user.friendIds.iterator(); iter.hasNext(); ) {
                    int friendId = iter.next();
                    boolean friendHasMasterHere = this.logicalPartitions.get(user.pid).contains(friendId);
                    boolean friendHasReplicaHere = this.logicalReplicaPartitions.get(user.pid).contains(friendId);
                    if(!friendHasMasterHere && !friendHasReplicaHere) {
                        throw new RuntimeException("User " + user.id + "'s friend " + friendId + " has no presence on " + user.pid);
                    }
                }
            }
        }

    }

    static class LogicalUser {
        private final int minNumReplicas;
        private final int id;
        private final int pid;
        private final float gamma;
        private final Integer totalWeight;
        private final TIntIntMap pToFriendCount;
        private final TIntIntMap pToWeight;
        private final TIntSet friendIds;

        private final TIntSet replicaLocations;
        private final TIntIntMap numFriendsToAddInEachPartition;
        private final int numFriendReplicasToDeleteInSourcePartition;
        private final boolean replicateInSourcePartition;

        public LogicalUser(int id, int pid, float gamma, TIntSet friendIds, TIntIntMap pToFriendCount, TIntIntMap pToWeight, TIntSet replicaLocations, TIntIntMap numFriendsToAddInEachPartition, int numFriendReplicasToDeleteInSourcePartition, boolean replicateInSourcePartition, int totalWeight, int minNumReplicas) {
            this.id = id;
            this.pid = pid;
            this.gamma = gamma;
            this.friendIds = friendIds;
            this.pToFriendCount = pToFriendCount;
            this.pToWeight = pToWeight;
            this.replicaLocations = replicaLocations;
            this.numFriendsToAddInEachPartition = numFriendsToAddInEachPartition;
            this.numFriendReplicasToDeleteInSourcePartition = numFriendReplicasToDeleteInSourcePartition;
            this.replicateInSourcePartition = replicateInSourcePartition;
            this.totalWeight = totalWeight;
            this.minNumReplicas = minNumReplicas;
        }

        public TIntSet getFriendIds() {
            return friendIds;
        }

        public int getId() {
            return id;
        }

        public int getPid() {
            return pid;
        }

        public Integer getTotalWeight() {
            return totalWeight;
        }

        public TIntIntMap getpToFriendCount() {
            return pToFriendCount;
        }

        public TIntIntMap getpToWeight() {
            return pToWeight;
        }

        public TIntSet getReplicaLocations() {
            return replicaLocations;
        }

        public TIntIntMap getNumFriendsToAddInEachPartition() {
            return numFriendsToAddInEachPartition;
        }

        public int getNumFriendReplicasToDeleteInSourcePartition() {
            return numFriendReplicasToDeleteInSourcePartition;
        }

        public boolean isReplicateInSourcePartition() {
            return replicateInSourcePartition;
        }

        private float getImbalanceFactor(Integer pId, Integer offset) {
            float partitionWeight = pToWeight.get(pId) + offset;
            float averageWeight = ((float) totalWeight) / pToWeight.keySet().size();
            return partitionWeight / averageWeight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LogicalUser that = (LogicalUser) o;

            if (id != that.id) return false;
            if (pid != that.pid) return false;
            if (Float.compare(that.gamma, gamma) != 0) return false;
            if (numFriendReplicasToDeleteInSourcePartition != that.numFriendReplicasToDeleteInSourcePartition) return false;
            if (replicateInSourcePartition != that.replicateInSourcePartition) return false;
            if (!totalWeight.equals(that.totalWeight)) return false;
            if (!pToFriendCount.equals(that.pToFriendCount)) return false;
            if (!pToWeight.equals(that.pToWeight)) return false;
            if (!replicaLocations.equals(that.replicaLocations)) return false;
            return numFriendsToAddInEachPartition.equals(that.numFriendsToAddInEachPartition);

        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + pid;
            result = 31 * result + (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
            result = 31 * result + totalWeight.hashCode();
            result = 31 * result + pToFriendCount.hashCode();
            result = 31 * result + pToWeight.hashCode();
            result = 31 * result + replicaLocations.hashCode();
            result = 31 * result + numFriendsToAddInEachPartition.hashCode();
            result = 31 * result + numFriendReplicasToDeleteInSourcePartition;
            result = 31 * result + (replicateInSourcePartition ? 1 : 0);
            return result;
        }

        public int getMinNumReplicas() {
            return minNumReplicas;
        }

        public Target getTarget(boolean firstStage) {
            boolean underweight = getImbalanceFactor(pid, -1) < (2-gamma);
            boolean overweight = getImbalanceFactor(pid, 0) > gamma;
            if(underweight) {
                return new Target(id, null, null, 0f);
            }

            Integer target = null;
            Integer maxGain = 0;

            if(overweight) {
                maxGain = Integer.MIN_VALUE;
            }

            for(int targetPid : pToFriendCount.keys()) {
                if((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) {
                    float balanceFactor = getImbalanceFactor(targetPid, 1);
                    int gain = calculateGain(targetPid);
                    if(gain > maxGain && balanceFactor < gamma) {
                        target = targetPid;
                        maxGain = gain;
                    }
                }
            }
            return new Target(id, target, pid, (float) maxGain);
        }

        private int calculateGain(Integer targetPid) {
            boolean deleteReplicaInTargetPartition = replicaLocations.contains(targetPid) && replicaLocations.size() > minNumReplicas; //TODO: this assumes we won't add other replicas, so it's not tight
            int numToDelete = numFriendReplicasToDeleteInSourcePartition + (deleteReplicaInTargetPartition ? 1 : 0);

            int numFriendReplicasToAddInTargetPartition = numFriendsToAddInEachPartition.get(targetPid);
            int numToAdd = numFriendReplicasToAddInTargetPartition + (replicateInSourcePartition ? 1 : 0);

            return numToDelete - numToAdd;
        }
    }

}
