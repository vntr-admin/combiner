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
 * Created by robertlindquist on 4/21/17.
 */
public class HRepartitioner {
    public static NoRepResults repartition(int k, int maxIterations, float gamma, TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> friendships) {
        int moves = 0;
        State state = initState(partitions, friendships, gamma);
        for(int i=0; i<maxIterations; i++) {
            int movesBeforeIteration = moves;

            Set<Target> firstStageTargets = performStage(true, k, state);
            moves += firstStageTargets.size();
            state.updateLogicalUsers(firstStageTargets, friendships);

            Set<Target> secondStageTargets = performStage(false, k, state);
            moves += secondStageTargets.size();
            state.updateLogicalUsers(secondStageTargets, friendships);

            if(moves == movesBeforeIteration) {
                break;
            }
        }

        return new NoRepResults(getUToMasterMap(state.getLogicalPartitions()), moves);
    }

    static Set<Target> performStage(boolean firstStage, int k, State state) {
        Set<Target> targets = new HashSet<>();
        for(int pid : state.getLogicalPartitions().keys()) {
            targets.addAll(getCandidates(pid, firstStage, k, state));
        }

        logicallyMigrate(targets, state);

        return targets;
    }

    static Set<Target> getCandidates(int pid, boolean firstIteration, int k, State state) {
        NavigableSet<Target> candidates = new TreeSet<>();
        for(TIntIterator iter = state.getLogicalPartitions().get(pid).iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            Target target = state.getLogicalUsers().get(uid).getTargetPart(firstIteration);
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

    static void logicallyMigrate(Set<Target> targets, State state) {
        for(Target target : targets) {
            state.getLogicalPartitions().get(target.oldPid).remove(target.uid);
            state.getLogicalPartitions().get(target.pid).add(target.uid);
        }
    }

    static State initState(TIntObjectMap<TIntSet> partitions, TIntObjectMap<TIntSet> friendships, float gamma) {
        State state = new State();
        TIntObjectMap<TIntSet> logicalPartitions = new TIntObjectHashMap<>(partitions.size() + 1);

        for(int pid : partitions.keys()) {
            TIntSet uids = partitions.get(pid);
            logicalPartitions.put(pid, new TIntHashSet(uids));
        }

        state.setLogicalPartitions(logicalPartitions);
        state.updateLogicalUsers(friendships, gamma);

        return state;
    }

    public static TIntObjectMap<LogicalUser> initLogicalUsers(TIntObjectMap<TIntSet> logicalPids, TIntObjectMap<TIntSet> friendships, TIntSet uidsToInclude, float gamma) {
        TIntIntMap uidToPidMap = getUToMasterMap(logicalPids);

        TIntIntMap pToWeight = new TIntIntHashMap(logicalPids.size() + 1);
        int totalWeight = 0;
        for(int pid : logicalPids.keys()) {
            int numUsersOnPartition = logicalPids.get(pid).size();
            pToWeight.put(pid, numUsersOnPartition);
            totalWeight += numUsersOnPartition;
        }

        TIntObjectMap<LogicalUser> logicalUsers = new TIntObjectHashMap<>();
        for(TIntIterator iter = uidsToInclude.iterator(); iter.hasNext(); ) {
            int uid = iter.next();
            TIntIntMap pToFriendCount = new TIntIntHashMap(logicalPids.size() + 1);
            for(int pid : logicalPids.keys()) {
                pToFriendCount.put(pid, 0);
            }
            for(TIntIterator iter2 = friendships.get(uid).iterator(); iter2.hasNext(); ) {
                int friendId = iter2.next();
                int pid = uidToPidMap.get(friendId);
                pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
            }

            int pid = uidToPidMap.get(uid);
            logicalUsers.put(uid, new LogicalUser(uid, pid, gamma, pToFriendCount, new TIntIntHashMap(pToWeight), totalWeight));
        }

        return logicalUsers;
    }

    static class State {

        private TIntObjectMap<TIntSet> logicalPartitions;
        private TIntObjectMap<LogicalUser> logicalUsers;
        public State() {
        }

        public TIntObjectMap<TIntSet> getLogicalPartitions() {
            return logicalPartitions;
        }

        public void setLogicalPartitions(TIntObjectMap<TIntSet> logicalPartitions) {
            this.logicalPartitions = logicalPartitions;
        }

        public TIntObjectMap<LogicalUser> getLogicalUsers() {
            return logicalUsers;
        }

        public void updateLogicalUsers(TIntObjectMap<TIntSet> friendships, float gamma) {
            setLogicalUsers(initLogicalUsers(logicalPartitions, friendships, friendships.keySet(), gamma));
        }

        public void updateLogicalUsers(Set<Target> targets, TIntObjectMap<TIntSet> friendships) {
            TIntIntMap pToWeight = getUserCounts(logicalPartitions);
            for(int uid : logicalUsers.keys()) {
                logicalUsers.get(uid).pToWeight = new TIntIntHashMap(pToWeight);
            }
            for(Target target : targets) {
                getLogicalUsers().get(target.uid).pid = target.pid;

                for(TIntIterator iter = friendships.get(target.uid).iterator(); iter.hasNext(); ) {
                    int friendId = iter.next();
                    TIntIntMap pToFriendCount = logicalUsers.get(friendId).pToFriendCount;
                    pToFriendCount.put(target.oldPid, pToFriendCount.get(target.oldPid) - 1);
                    pToFriendCount.put(target.pid, pToFriendCount.get(target.pid) + 1);
                }
            }
        }

        public void setLogicalUsers(TIntObjectMap<LogicalUser> logicalUsers) {
            this.logicalUsers = logicalUsers;
        }

    }

    public static class LogicalUser {
        private final Integer id;
        private Integer pid;
        private float gamma;
        private TIntIntMap pToFriendCount;
        private TIntIntMap pToWeight;
        private final Integer totalWeight;

        public LogicalUser(Integer id, Integer pid, float gamma, TIntIntMap pToFriendCount, TIntIntMap pToWeight, Integer totalWeight) {
            this.id = id;
            this.pid = pid;
            this.gamma = gamma;
            this.pToFriendCount = pToFriendCount;
            this.pToWeight = pToWeight;
            this.totalWeight = totalWeight;
        }

        public Integer getId() {
            return id;
        }

        public TIntIntMap getpToWeight() {
            return pToWeight;
        }

        public Integer getTotalWeight() {
            return totalWeight;
        }

        public Integer getPid() {
            return pid;
        }

        public TIntIntMap getpToFriendCount() {
            return pToFriendCount;
        }

        public Target getTargetPart(boolean firstStage) {
            boolean underweight = getImbalanceFactor(pid, -1) < (2-gamma);
            boolean overweight  = getImbalanceFactor(pid, 0)  > gamma;

            if(underweight) {
                return new Target(id, null, null, 0f);
            }

            TIntSet targets = new TIntHashSet();
            Integer maxGain = overweight ? Integer.MIN_VALUE : 0;

            for(Integer targetPid : pToFriendCount.keys()) {

                if((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) {

                    int gain = pToFriendCount.get(targetPid) - pToFriendCount.get(pid);
                    float balanceFactor = getImbalanceFactor(targetPid, 1);

                    if (gain > maxGain && balanceFactor < gamma) {
                        targets = new TIntHashSet();
                        targets.add(targetPid);
                        maxGain = gain;
                    } else if (gain == maxGain && (overweight || gain > 0)) {
                        targets.add(targetPid);
                    }
                }
            }

            Integer targetPid = null;
            if(!targets.isEmpty()) {
                int minWeightForPartition = Integer.MAX_VALUE;
                for(TIntIterator iter = targets.iterator(); iter.hasNext(); ) {
                    int curTarget = iter.next();
                    int curWeight = pToWeight.get(curTarget);
                    if(curWeight < minWeightForPartition) {
                        minWeightForPartition = curWeight;
                        targetPid = curTarget;
                    }
                }
            }

            return new Target(id, targetPid, pid, (float) maxGain);
        }

        public float getImbalanceFactor(Integer pId, Integer offset) {
            float partitionWeight = pToWeight.get(pId) + offset;
            float averageWeight = ((float) totalWeight) / pToWeight.keySet().size();
            return partitionWeight / averageWeight;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LogicalUser that = (LogicalUser) o;

            if (Float.compare(that.gamma, gamma) != 0) return false;
            if (!id.equals(that.id)) return false;
            if (!pid.equals(that.pid)) return false;
            if (!pToFriendCount.equals(that.pToFriendCount)) return false;
            if (!pToWeight.equals(that.pToWeight)) return false;
            return totalWeight.equals(that.totalWeight);

        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + pid.hashCode();
            result = 31 * result + (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
            result = 31 * result + pToFriendCount.hashCode();
            result = 31 * result + pToWeight.hashCode();
            result = 31 * result + totalWeight.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "u" + id + "|, p" + pid + "|" + pToFriendCount;
        }
    }

}
