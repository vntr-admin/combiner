package io.vntr.repartition;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

import java.util.*;

import static io.vntr.utils.TroveUtils.*;

/**
 * Created by robertlindquist on 4/22/17.
 */
public class SparmesRepartitioner {

    public static RepResults repartition(short k, int maxIterations, float gamma, short minNumReplicas, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas, TShortObjectMap<TShortSet> friendships) {
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

        TShortShortMap uidsToPids = getUToMasterMap(state.getLogicalPartitions());
        TShortObjectMap<TShortSet> uidsToReplicaPids = getUToReplicasMap(state.getLogicalReplicaPartitions(), friendships.keySet());

        return new RepResults(moves, uidsToPids, uidsToReplicaPids);
    }

    static int performStage(boolean firstStage, short k, State state) {
        Set<Target> targets = new HashSet<>();

        for(short pid : state.getLogicalPartitions().keys()) {
            targets.addAll(getPartitionCandidates(pid, firstStage, k, state));
        }

        TShortObjectMap<TShortSet> uidToReplicaPids = getUToReplicasMap(state.getLogicalReplicaPartitions(), state.getLogicalUsers().keySet());
        for(Target target : targets) {
            migrateLogically(target, state, uidToReplicaPids);
        }

        return targets.size();
    }

    static Set<Target> getPartitionCandidates(short pid, boolean firstIteration, short k, State state) {
        NavigableSet<Target> candidates = new TreeSet<>();
        for(TShortIterator iter = state.getLogicalPartitions().get(pid).iterator(); iter.hasNext(); ) {
            short uid = iter.next();
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

    static void migrateLogically(Target target, State state, TShortObjectMap<TShortSet> uidToReplicaPids) {
        TShortObjectMap<TShortSet> logicalPids = state.getLogicalPartitions();
        TShortObjectMap<TShortSet> logicalReplicaPids = state.getLogicalReplicaPartitions();

        TShortSet oldMasters = logicalPids.get(target.oldPid);
        TShortSet newMasters = logicalPids.get(target.pid);
        TShortSet oldReplicas = logicalReplicaPids.get(target.oldPid);
        TShortSet newReplicas = logicalReplicaPids.get(target.pid);

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
        for(TShortIterator iter = user.getFriendIds().iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            if(!newMasters.contains(friendId) && !newReplicas.contains(friendId)) {
                newReplicas.add(friendId);
            }
        }


        //Remove replicas as allowed

        //First, remove user replica from new partition if one exists
        newReplicas.remove(target.uid); //doesn't cause a problem if it wasn't already there

        //Second, if we've violated k-constraints, choose another partition at random and replicate this user there
        TShortSet replicaPids = uidToReplicaPids.get(target.uid);
        if(replicaPids.size() < user.getMinNumReplicas()) {
            TShortSet potentialReplicaLocations = new TShortHashSet(logicalReplicaPids.keySet());
            potentialReplicaLocations.remove(target.pid);
            potentialReplicaLocations.removeAll(replicaPids);
            short newReplicaPid = getRandomElement(potentialReplicaLocations);
            logicalReplicaPids.get(newReplicaPid).add(target.uid);
        }

        //Third, remove friends replicas from old partition if they weren't being used for any other reason and don't violate k-replication
        TShortSet friendReplicasToRemoveFromOldPartition = new TShortHashSet(user.getFriendIds());
        friendReplicasToRemoveFromOldPartition.retainAll(oldReplicas);

        for(TShortIterator iter = friendReplicasToRemoveFromOldPartition.iterator(); iter.hasNext(); ) {
            short friendId = iter.next();
            TShortSet friendsOfFriend = state.getLogicalUsers().get(friendId).getFriendIds();
            if(disjoint(friendsOfFriend, oldMasters)) {
                int numFriendReplicas = uidToReplicaPids.get(friendId).size();
                if(numFriendReplicas > user.getMinNumReplicas()) {
                    oldReplicas.remove(friendId);
                }
            }
        }
    }

    static class State {
        private short minNumReplicas;
        private float gamma;
        private TShortObjectMap<TShortSet> friendships;

        private TShortObjectMap<LogicalUser> logicalUsers;
        private TShortObjectMap<TShortSet> logicalPartitions;
        private TShortObjectMap<TShortSet> logicalReplicaPartitions;

        public State() {
        }

        public TShortObjectMap<LogicalUser> getLogicalUsers() {
            return logicalUsers;
        }

        public void setLogicalUsers(TShortObjectMap<LogicalUser> logicalUsers) {
            this.logicalUsers = logicalUsers;
        }

        public TShortObjectMap<TShortSet> getLogicalPartitions() {
            return logicalPartitions;
        }

        public void setLogicalPartitions(TShortObjectMap<TShortSet> logicalPartitions) {
            this.logicalPartitions = logicalPartitions;
        }

        public TShortObjectMap<TShortSet> getLogicalReplicaPartitions() {
            return logicalReplicaPartitions;
        }

        public void setLogicalReplicaPartitions(TShortObjectMap<TShortSet> logicalReplicaPartitions) {
            this.logicalReplicaPartitions = logicalReplicaPartitions;
        }

        static State init(short minNumReplicas, float gamma, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas, TShortObjectMap<TShortSet> friendships) {
            State state = new State();
            state.minNumReplicas = minNumReplicas;
            state.gamma = gamma;
            state.friendships = friendships;

            TShortObjectMap<TShortSet> logicalPartitions = new TShortObjectHashMap<>(partitions.size()+1);
            for(short pid : partitions.keys()) {
                logicalPartitions.put(pid, new TShortHashSet(partitions.get(pid)));
            }
            state.setLogicalPartitions(logicalPartitions);

            TShortObjectMap<TShortSet> logicalReplicas = new TShortObjectHashMap<>(partitions.size()+1);
            for(short pid : replicas.keys()) {
                logicalReplicas.put(pid, new TShortHashSet(replicas.get(pid)));
            }
            state.setLogicalReplicaPartitions(logicalReplicas);

            state.updateLogicalUsers();

            return state;
        }

        void updateLogicalUsers() {
            setLogicalUsers(initLogicalUsers(minNumReplicas, gamma, logicalPartitions, logicalReplicaPartitions, friendships));
        }

        static TShortObjectMap<LogicalUser> initLogicalUsers(short minNumReplicas, float gamma, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> replicas, TShortObjectMap<TShortSet> friendships) {
            TShortObjectMap<LogicalUser> logicalUsers = new TShortObjectHashMap<>(friendships.size()+1);

            short maxPid = max(partitions.keySet());
            short maxUid = max(friendships.keySet());

            TShortShortMap uidToPidMap = getUToMasterMap(partitions);
            TShortObjectMap<TShortSet> uidToReplicaMap = getUToReplicasMap(replicas, friendships.keySet());
            short[] numDeletionCandidates = getNumDeletionCandidates(maxUid, minNumReplicas, friendships, replicas, uidToPidMap, uidToReplicaMap);
            TShortObjectMap<TShortShortMap> uidToNumFriendsToAddInEachPartition = getUidToNumFriendsToAddInEachPartition(friendships, uidToReplicaMap, uidToPidMap, partitions.keySet(), maxPid);

            TShortShortMap pToWeight = getUserCounts(partitions);
            int totalWeight = 0;
            for(short key : pToWeight.keys()) {
                totalWeight += pToWeight.get(key);
            }

            for(short uid : friendships.keys()) {
                TShortSet friendIds = new TShortHashSet(friendships.get(uid));
                TShortShortMap pToFriendCount = getPToFriendCount(friendIds, uidToPidMap, partitions.keySet(), maxPid);

                short numFriendReplicasToDeleteInSourcePartition = numDeletionCandidates[uid];
                boolean replicateInSourcePartition = !disjoint(friendIds, partitions.get(uidToPidMap.get(uid)));

                LogicalUser user = new LogicalUser(
                        uid,
                        uidToPidMap.get(uid),
                        gamma,
                        friendIds,
                        pToFriendCount,
                        new TShortShortHashMap(pToWeight),
                        new TShortHashSet(uidToReplicaMap.get(uid)),
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

        static short[] getNumDeletionCandidates(short maxUid, short minNumReplicas, TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> replicas, TShortShortMap uidToPidMap, TShortObjectMap<TShortSet> uidToReplicasMap) {
            short[] numDeletionCandidates = new short[maxUid+1];

            for(short pid : replicas.keys()) {
middle:         for(TShortIterator iter = replicas.get(pid).iterator(); iter.hasNext(); ) {
                    short replicaId = iter.next();
                    Short friendOnPartition = null;

                    for(TShortIterator iter2 = friendships.get(replicaId).iterator(); iter2.hasNext(); ) {
                        short friendId = iter2.next();
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

        static TShortObjectMap<TShortShortMap> getUidToNumFriendsToAddInEachPartition(TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> uidToReplicaMap, TShortShortMap uidToPidMap, TShortSet pids, int maxPid) {
            TShortObjectMap<TShortShortMap> uidToNumFriendsToAddInEachPartition = new TShortObjectHashMap<>(friendships.size()+1);

            for(short uid : friendships.keys()) {
                //you have to add a replica of each friend who isn't present in the partition
                TShortSet friendIds = friendships.get(uid);
                short[] friendsToAdd = new short[maxPid+1];
                Arrays.fill(friendsToAdd, (short)friendIds.size());

                for(TShortIterator iter = friendIds.iterator(); iter.hasNext(); ) {
                    short friendId = iter.next();
                    for(TShortIterator iter2 = uidToReplicaMap.get(friendId).iterator(); iter2.hasNext(); ) {
                        int friendLocation = iter2.next();
                        friendsToAdd[friendLocation]--;
                    }
                    friendsToAdd[uidToPidMap.get(friendId)]--;
                }

                int pid = uidToPidMap.get(uid);
                TShortShortMap numFriendsToAddInEachPartition = new TShortShortHashMap(pids.size()+1);
                for(TShortIterator iter = pids.iterator(); iter.hasNext(); ) {
                    short pid1 = iter.next();
                    if(pid1 != pid) {
                        numFriendsToAddInEachPartition.put(pid1, friendsToAdd[pid1]);
                    }
                }
                uidToNumFriendsToAddInEachPartition.put(uid, numFriendsToAddInEachPartition);
            }
            return uidToNumFriendsToAddInEachPartition;
        }

        static TShortShortMap getPToFriendCount(TShortSet friendIds, TShortShortMap uidToPidMap, TShortSet pids, int maxPid) {
            short[] friendCounts = new short[maxPid+1];
            for(TShortIterator iter = friendIds.iterator(); iter.hasNext(); ) {
                short friendId = iter.next();
                friendCounts[uidToPidMap.get(friendId)]++;
            }
            TShortShortMap pToFriendCount = new TShortShortHashMap(pids.size()+1);
            for(TShortIterator iter = pids.iterator(); iter.hasNext(); ) {
                short pid = iter.next();
                pToFriendCount.put(pid, friendCounts[pid]);
            }
            return pToFriendCount;
        }

        void checkStateValidity() {
            for(LogicalUser user : this.getLogicalUsers().valueCollection()) {
                if(user.replicaLocations.size() < this.minNumReplicas) {
                    throw new RuntimeException("User " + user.id + " has " + this.minNumReplicas + "-replication problem.");
                }
                for(short pid : this.getLogicalPartitions().keys()) {
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
                for(TShortIterator iter = user.friendIds.iterator(); iter.hasNext(); ) {
                    short friendId = iter.next();
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
        private final short minNumReplicas;
        private final short id;
        private final short pid;
        private final float gamma;
        private final int totalWeight;
        private final TShortShortMap pToFriendCount;
        private final TShortShortMap pToWeight;
        private final TShortSet friendIds;

        private final TShortSet replicaLocations;
        private final TShortShortMap numFriendsToAddInEachPartition;
        private final int numFriendReplicasToDeleteInSourcePartition;
        private final boolean replicateInSourcePartition;

        public LogicalUser(short id, short pid, float gamma, TShortSet friendIds, TShortShortMap pToFriendCount, TShortShortMap pToWeight, TShortSet replicaLocations, TShortShortMap numFriendsToAddInEachPartition, int numFriendReplicasToDeleteInSourcePartition, boolean replicateInSourcePartition, int totalWeight, short minNumReplicas) {
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

        public TShortSet getFriendIds() {
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

        public TShortShortMap getpToFriendCount() {
            return pToFriendCount;
        }

        public TShortShortMap getpToWeight() {
            return pToWeight;
        }

        public TShortSet getReplicaLocations() {
            return replicaLocations;
        }

        public TShortShortMap getNumFriendsToAddInEachPartition() {
            return numFriendsToAddInEachPartition;
        }

        public int getNumFriendReplicasToDeleteInSourcePartition() {
            return numFriendReplicasToDeleteInSourcePartition;
        }

        public boolean isReplicateInSourcePartition() {
            return replicateInSourcePartition;
        }

        private float getImbalanceFactor(short pId, short offset) {
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
            if (totalWeight != that.totalWeight) return false;
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
            result = 31 * result + totalWeight;
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
            boolean underweight = getImbalanceFactor(pid, (short) -1) < (2-gamma);
            boolean overweight = getImbalanceFactor(pid, (short) 0) > gamma;
            if(underweight) {
                return new Target(id, null, null, 0f);
            }

            Short target = null;
            short maxGain = 0;

            if(overweight) {
                maxGain = Short.MIN_VALUE;
            }

            for(short targetPid : pToFriendCount.keys()) {
                if((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) {
                    float balanceFactor = getImbalanceFactor(targetPid, (short)1);
                    short gain = calculateGain(targetPid);
                    if(gain > maxGain && balanceFactor < gamma) {
                        target = targetPid;
                        maxGain = gain;
                    }
                }
            }
            return new Target(id, target, pid, (float) maxGain);
        }

        private short calculateGain(short targetPid) {
            boolean deleteReplicaInTargetPartition = replicaLocations.contains(targetPid) && replicaLocations.size() > minNumReplicas; //TODO: this assumes we won't add other replicas, so it's not tight
            int numToDelete = numFriendReplicasToDeleteInSourcePartition + (deleteReplicaInTargetPartition ? 1 : 0);

            int numFriendReplicasToAddInTargetPartition = numFriendsToAddInEachPartition.get(targetPid);
            int numToAdd = numFriendReplicasToAddInTargetPartition + (replicateInSourcePartition ? 1 : 0);

            return (short)(numToDelete - numToAdd);
        }
    }

}
