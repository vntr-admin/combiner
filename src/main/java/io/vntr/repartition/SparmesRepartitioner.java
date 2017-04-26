package io.vntr.repartition;

import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.utils.Utils.*;
import static java.util.Collections.disjoint;

/**
 * Created by robertlindquist on 4/22/17.
 */
public class SparmesRepartitioner {

    public static Results repartition(int k, int maxIterations, float gamma, int minNumReplicas, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas, Map<Integer, Set<Integer>> friendships) {
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

        Map<Integer, Integer> uidsToPids = getUToMasterMap(state.getLogicalPartitions());
        Map<Integer, Set<Integer>> uidsToReplicaPids = getUToReplicasMap(state.getLogicalReplicaPartitions(), friendships.keySet());

        return new Results(moves, uidsToPids, uidsToReplicaPids);
    }

    static int performStage(boolean firstStage, int k, State state) {
        Set<Target> targets = new HashSet<>();

        for(int pid : state.getLogicalPartitions().keySet()) {
            targets.addAll(getPartitionCandidates(pid, firstStage, k, state));
        }

        Map<Integer, Set<Integer>> uidToReplicaPids = getUToReplicasMap(state.getLogicalReplicaPartitions(), state.getLogicalUsers().keySet());
        for(Target target : targets) {
            migrateLogically(target, state, uidToReplicaPids);
        }

        return targets.size();
    }

    static Set<Target> getPartitionCandidates(int pid, boolean firstIteration, int k, State state) {
        NavigableSet<Target> candidates = new TreeSet<>();
        for(int uid : state.getLogicalPartitions().get(pid)) {
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

    static void migrateLogically(Target target, State state, Map<Integer, Set<Integer>> uidToReplicaPids) {
        Map<Integer, Set<Integer>> logicalPids = state.getLogicalPartitions();
        Map<Integer, Set<Integer>> logicalReplicaPids = state.getLogicalReplicaPartitions();

        Set<Integer> oldMasters = logicalPids.get(target.oldPid);
        Set<Integer> newMasters = logicalPids.get(target.pid);
        Set<Integer> oldReplicas = logicalReplicaPids.get(target.oldPid);
        Set<Integer> newReplicas = logicalReplicaPids.get(target.pid);

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
        for(int friendId : user.getFriendIds()) {
            if(!newMasters.contains(friendId) && !newReplicas.contains(friendId)) {
                newReplicas.add(friendId);
            }
        }


        //Remove replicas as allowed

        //First, remove user replica from new partition if one exists
        newReplicas.remove(target.uid); //doesn't cause a problem if it wasn't already there

        //Second, if we've violated k-constraints, choose another partition at random and replicate this user there
        Set<Integer> replicaPids = uidToReplicaPids.get(target.uid);
        if(replicaPids.size() < user.getMinNumReplicas()) {
            Set<Integer> potentialReplicaLocations = new HashSet<>(logicalReplicaPids.keySet());
            potentialReplicaLocations.remove(target.pid);
            potentialReplicaLocations.removeAll(replicaPids);
            int newReplicaPid = ProbabilityUtils.getRandomElement(potentialReplicaLocations);
            logicalReplicaPids.get(newReplicaPid).add(target.uid);
        }

        //Third, remove friends replicas from old partition if they weren't being used for any other reason and don't violate k-replication
        Set<Integer> friendReplicasToRemoveFromOldPartition = new HashSet<>(user.getFriendIds());
        friendReplicasToRemoveFromOldPartition.retainAll(oldReplicas);
        for(Integer friendId : friendReplicasToRemoveFromOldPartition) {
            Set<Integer> friendsOfFriend = state.getLogicalUsers().get(friendId).getFriendIds();
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
        private Map<Integer, Set<Integer>> friendships;

        private Map<Integer, LogicalUser> logicalUsers;
        private Map<Integer, Set<Integer>> logicalPartitions;
        private Map<Integer, Set<Integer>> logicalReplicaPartitions;

        public State() {
        }

        public Map<Integer, LogicalUser> getLogicalUsers() {
            return logicalUsers;
        }

        public void setLogicalUsers(Map<Integer, LogicalUser> logicalUsers) {
            this.logicalUsers = logicalUsers;
        }

        public Map<Integer, Set<Integer>> getLogicalPartitions() {
            return logicalPartitions;
        }

        public void setLogicalPartitions(Map<Integer, Set<Integer>> logicalPartitions) {
            this.logicalPartitions = logicalPartitions;
        }

        public Map<Integer, Set<Integer>> getLogicalReplicaPartitions() {
            return logicalReplicaPartitions;
        }

        public void setLogicalReplicaPartitions(Map<Integer, Set<Integer>> logicalReplicaPartitions) {
            this.logicalReplicaPartitions = logicalReplicaPartitions;
        }

        static State init(int minNumReplicas, float gamma, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas, Map<Integer, Set<Integer>> friendships) {
            State state = new State();
            state.minNumReplicas = minNumReplicas;
            state.gamma = gamma;
            state.friendships = friendships;

            Map<Integer, Set<Integer>> logicalPartitions = new HashMap<>();
            for(int pid : partitions.keySet()) {
                logicalPartitions.put(pid, new HashSet<>(partitions.get(pid)));
            }
            state.setLogicalPartitions(logicalPartitions);

            Map<Integer, Set<Integer>> logicalReplicas = new HashMap<>();
            for(int pid : replicas.keySet()) {
                logicalReplicas.put(pid, new HashSet<>(replicas.get(pid)));
            }
            state.setLogicalReplicaPartitions(logicalReplicas);

            state.updateLogicalUsers();

            return state;
        }

        void updateLogicalUsers() {
            setLogicalUsers(initLogicalUsers(minNumReplicas, gamma, logicalPartitions, logicalReplicaPartitions, friendships));
        }

        static Map<Integer, LogicalUser> initLogicalUsers(int minNumReplicas, float gamma, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas, Map<Integer, Set<Integer>> friendships) {
            Map<Integer, LogicalUser> logicalUsers = new HashMap<>();

            int maxPid = Collections.max(partitions.keySet());
            int maxUid = Collections.max(friendships.keySet());

            Map<Integer, Integer> uidToPidMap = getUToMasterMap(partitions);
            Map<Integer, Set<Integer>> uidToReplicaMap = getUToReplicasMap(replicas, friendships.keySet());
            int[] numDeletionCandidates = getNumDeletionCandidates(maxUid, minNumReplicas, friendships, replicas, uidToPidMap, uidToReplicaMap);
            Map<Integer, Map<Integer, Integer>> uidToNumFriendsToAddInEachPartition = getUidToNumFriendsToAddInEachPartition(friendships, uidToReplicaMap, uidToPidMap, partitions.keySet(), maxPid);

            Map<Integer, Integer> pToWeight = getUserCounts(partitions);
            int totalWeight = 0;
            for(int weight : pToWeight.values()) {
                totalWeight += weight;
            }

            for(int uid : friendships.keySet()) {
                Set<Integer> friendIds = new HashSet<>(friendships.get(uid));
                Map<Integer, Integer> pToFriendCount = getPToFriendCount(friendIds, uidToPidMap, partitions.keySet(), maxPid);

                Set<Integer> replicaLocations = new HashSet<>();
                for(int pid : replicas.keySet()) {
                    if(replicas.get(pid).contains(uid)) {
                        replicaLocations.add(pid);
                    }
                }

                int numFriendReplicasToDeleteInSourcePartition = numDeletionCandidates[uid];
                boolean replicateInSourcePartition = !disjoint(friendIds, partitions.get(uidToPidMap.get(uid)));

                LogicalUser user = new LogicalUser(
                        uid,
                        uidToPidMap.get(uid),
                        gamma,
                        friendIds,
                        pToFriendCount,
                        new HashMap<>(pToWeight),
                        replicaLocations,
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

        static int[] getNumDeletionCandidates(int maxUid, int minNumReplicas, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> replicas, Map<Integer, Integer> uidToPidMap, Map<Integer, Set<Integer>> uidToReplicasMap) {
            int[] numDeletionCandidates = new int[maxUid+1];

            for(int pid : replicas.keySet()) {
middle:         for(int replicaId : replicas.get(pid)) {
                    Integer friendOnPartition = null;
                    for(int friendId : friendships.get(replicaId)) {
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

        static Map<Integer, Map<Integer, Integer>> getUidToNumFriendsToAddInEachPartition(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> uidToReplicaMap, Map<Integer, Integer> uidToPidMap, Set<Integer> pids, int maxPid) {
            Map<Integer, Map<Integer, Integer>> uidToNumFriendsToAddInEachPartition = new HashMap<>();
            for(int uid : friendships.keySet()) {

                //you have to add a replica of each friend who isn't present in the partition
                Set<Integer> friendIds = friendships.get(uid);
                int[] friendsToAdd = new int[maxPid+1];
                Arrays.fill(friendsToAdd, friendIds.size());

                for(int friendId : friendIds) {
                    for(int friendLocation : uidToReplicaMap.get(friendId)) {
                        friendsToAdd[friendLocation]--;
                    }
                    friendsToAdd[uidToPidMap.get(friendId)]--;
                }

                int pid = uidToPidMap.get(uid);
                Map<Integer, Integer> numFriendsToAddInEachPartition = new HashMap<>();
                for(int pid1 : pids) {
                    if(pid1 != pid) {
                        numFriendsToAddInEachPartition.put(pid1, friendsToAdd[pid1]);
                    }
                }
                uidToNumFriendsToAddInEachPartition.put(uid, numFriendsToAddInEachPartition);
            }
            return uidToNumFriendsToAddInEachPartition;
        }

        static Map<Integer, Integer> getPToFriendCount(Set<Integer> friendIds, Map<Integer, Integer> uidToPidMap, Set<Integer> pids, int maxPid) {
            int[] friendCounts = new int[maxPid+1];
            for(Integer friendId : friendIds) {
                friendCounts[uidToPidMap.get(friendId)]++;
            }
            Map<Integer, Integer> pToFriendCount = new HashMap<>();
            for(int pid : pids) {
                pToFriendCount.put(pid, friendCounts[pid]);
            }
            return pToFriendCount;
        }

        void checkStateValidity() {
            for(LogicalUser user : this.getLogicalUsers().values()) {
                if(user.replicaLocations.size() < this.minNumReplicas) {
                    throw new RuntimeException("User " + user.id + " has " + this.minNumReplicas + "-replication problem.");
                }
                for(int pid : this.getLogicalPartitions().keySet()) {
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
                for(int friendId : user.friendIds) {
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
        private float gamma;
        private final Integer totalWeight;
        private final Map<Integer, Integer> pToFriendCount;
        private final Map<Integer, Integer> pToWeight;
        private final Set<Integer> friendIds;

        private final Set<Integer> replicaLocations;
        private final Map<Integer, Integer> numFriendsToAddInEachPartition;
        private final int numFriendReplicasToDeleteInSourcePartition;
        private final boolean replicateInSourcePartition;

        public LogicalUser(int id, int pid, float gamma, Set<Integer> friendIds, Map<Integer, Integer> pToFriendCount, Map<Integer, Integer> pToWeight, Set<Integer> replicaLocations, Map<Integer, Integer> numFriendsToAddInEachPartition, int numFriendReplicasToDeleteInSourcePartition, boolean replicateInSourcePartition, int totalWeight, int minNumReplicas) {
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

        public Set<Integer> getFriendIds() {
            return friendIds;
        }

        public int getId() {
            return id;
        }

        public int getPid() {
            return pid;
        }

        public float getGamma() {
            return gamma;
        }

        public Integer getTotalWeight() {
            return totalWeight;
        }

        public Map<Integer, Integer> getpToFriendCount() {
            return pToFriendCount;
        }

        public Map<Integer, Integer> getpToWeight() {
            return pToWeight;
        }

        public Set<Integer> getReplicaLocations() {
            return replicaLocations;
        }

        public Map<Integer, Integer> getNumFriendsToAddInEachPartition() {
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

            for(Integer targetPid : pToFriendCount.keySet()) {
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

    public static class Results {
        private final int numLogicalMoves;
        private final Map<Integer, Integer> uidsToPids;
        private final Map<Integer, Set<Integer>> uidsToReplicaPids;

        public Results(int numLogicalMoves, Map<Integer, Integer> uidsToPids, Map<Integer, Set<Integer>> uidsToReplicaPids) {
            this.numLogicalMoves = numLogicalMoves;
            this.uidsToPids = uidsToPids;
            this.uidsToReplicaPids = uidsToReplicaPids;
        }

        public int getNumLogicalMoves() {
            return numLogicalMoves;
        }

        public Map<Integer, Integer> getUidsToPids() {
            return uidsToPids;
        }

        public Map<Integer, Set<Integer>> getUidsToReplicaPids() {
            return uidsToReplicaPids;
        }
    }

}
