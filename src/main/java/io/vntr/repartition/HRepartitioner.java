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
 * Created by robertlindquist on 4/21/17.
 */
public class HRepartitioner {
    public static NoRepResults repartition(short k, short maxIterations, float gamma, TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> friendships) {
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

    static Set<Target> performStage(boolean firstStage, short k, State state) {
        Set<Target> targets = new HashSet<>();
        for(short pid : state.getLogicalPartitions().keys()) {
            targets.addAll(getCandidates(pid, firstStage, k, state));
        }

        logicallyMigrate(targets, state);

        return targets;
    }

    static Set<Target> getCandidates(short pid, boolean firstIteration, short k, State state) {
        NavigableSet<Target> candidates = new TreeSet<>();
        for(TShortIterator iter = state.getLogicalPartitions().get(pid).iterator(); iter.hasNext(); ) {
            short uid = iter.next();
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

    static State initState(TShortObjectMap<TShortSet> partitions, TShortObjectMap<TShortSet> friendships, float gamma) {
        State state = new State();
        TShortObjectMap<TShortSet> logicalPartitions = new TShortObjectHashMap<>(partitions.size() + 1);

        for(short pid : partitions.keys()) {
            TShortSet uids = partitions.get(pid);
            logicalPartitions.put(pid, new TShortHashSet(uids));
        }

        state.setLogicalPartitions(logicalPartitions);
        state.updateLogicalUsers(friendships, gamma);

        return state;
    }

    public static TShortObjectMap<LogicalUser> initLogicalUsers(TShortObjectMap<TShortSet> logicalPids, TShortObjectMap<TShortSet> friendships, TShortSet uidsToInclude, float gamma) {
        TShortShortMap uidToPidMap = getUToMasterMap(logicalPids);

        TShortShortMap pToWeight = new TShortShortHashMap(logicalPids.size() + 1);
        int totalWeight = 0;
        for(short pid : logicalPids.keys()) {
            short numUsersOnPartition = (short) logicalPids.get(pid).size();
            pToWeight.put(pid, numUsersOnPartition);
            totalWeight += numUsersOnPartition;
        }

        TShortObjectMap<LogicalUser> logicalUsers = new TShortObjectHashMap<>();
        for(TShortIterator iter = uidsToInclude.iterator(); iter.hasNext(); ) {
            short uid = iter.next();
            TShortShortMap pToFriendCount = new TShortShortHashMap(logicalPids.size() + 1);
            for(short pid : logicalPids.keys()) {
                pToFriendCount.put(pid, (short) 0);
            }
            for(TShortIterator iter2 = friendships.get(uid).iterator(); iter2.hasNext(); ) {
                short friendId = iter2.next();
                short pid = uidToPidMap.get(friendId);
                pToFriendCount.put(pid, (short)(pToFriendCount.get(pid) + 1));
            }

            short pid = uidToPidMap.get(uid);
            logicalUsers.put(uid, new LogicalUser(uid, pid, gamma, pToFriendCount, new TShortShortHashMap(pToWeight), totalWeight));
        }

        return logicalUsers;
    }

    static class State {

        private TShortObjectMap<TShortSet> logicalPartitions;
        private TShortObjectMap<LogicalUser> logicalUsers;
        public State() {
        }

        public TShortObjectMap<TShortSet> getLogicalPartitions() {
            return logicalPartitions;
        }

        public void setLogicalPartitions(TShortObjectMap<TShortSet> logicalPartitions) {
            this.logicalPartitions = logicalPartitions;
        }

        public TShortObjectMap<LogicalUser> getLogicalUsers() {
            return logicalUsers;
        }

        public void updateLogicalUsers(TShortObjectMap<TShortSet> friendships, float gamma) {
            setLogicalUsers(initLogicalUsers(logicalPartitions, friendships, friendships.keySet(), gamma));
        }

        public void updateLogicalUsers(Set<Target> targets, TShortObjectMap<TShortSet> friendships) {
            TShortShortMap pToWeight = getUserCounts(logicalPartitions);
            for(short uid : logicalUsers.keys()) {
                logicalUsers.get(uid).pToWeight = new TShortShortHashMap(pToWeight);
            }
            for(Target target : targets) {
                getLogicalUsers().get(target.uid).pid = target.pid;

                for(TShortIterator iter = friendships.get(target.uid).iterator(); iter.hasNext(); ) {
                    short friendId = iter.next();
                    TShortShortMap pToFriendCount = logicalUsers.get(friendId).pToFriendCount;
                    pToFriendCount.put(target.oldPid, (short)(pToFriendCount.get(target.oldPid) - 1));
                    pToFriendCount.put(target.pid, (short)(pToFriendCount.get(target.pid) + 1));
                }
            }
        }

        public void setLogicalUsers(TShortObjectMap<LogicalUser> logicalUsers) {
            this.logicalUsers = logicalUsers;
        }

    }

    public static class LogicalUser {
        private final short id;
        private short pid;
        private float gamma;
        private TShortShortMap pToFriendCount;
        private TShortShortMap pToWeight;
        private final int totalWeight;

        public LogicalUser(short id, short pid, float gamma, TShortShortMap pToFriendCount, TShortShortMap pToWeight, int totalWeight) {
            this.id = id;
            this.pid = pid;
            this.gamma = gamma;
            this.pToFriendCount = pToFriendCount;
            this.pToWeight = pToWeight;
            this.totalWeight = totalWeight;
        }

        public short getId() {
            return id;
        }

        public TShortShortMap getpToWeight() {
            return pToWeight;
        }

        public int getTotalWeight() {
            return totalWeight;
        }

        public short getPid() {
            return pid;
        }

        public TShortShortMap getpToFriendCount() {
            return pToFriendCount;
        }

        public Target getTargetPart(boolean firstStage) {
            boolean underweight = getImbalanceFactor(pid, (short)-1) < (2-gamma);
            boolean overweight  = getImbalanceFactor(pid, (short) 0)  > gamma;

            if(underweight) {
                return new Target(id, null, null, 0f);
            }

            TShortSet targets = new TShortHashSet();
            Integer maxGain = overweight ? Integer.MIN_VALUE : 0;

            for(short targetPid : pToFriendCount.keys()) {

                if((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) {

                    int gain = pToFriendCount.get(targetPid) - pToFriendCount.get(pid);
                    float balanceFactor = getImbalanceFactor(targetPid, (short)1);

                    if (gain > maxGain && balanceFactor < gamma) {
                        targets = new TShortHashSet();
                        targets.add(targetPid);
                        maxGain = gain;
                    } else if (gain == maxGain && (overweight || gain > 0)) {
                        targets.add(targetPid);
                    }
                }
            }

            Short targetPid = null;
            if(!targets.isEmpty()) {
                int minWeightForPartition = Integer.MAX_VALUE;
                for(TShortIterator iter = targets.iterator(); iter.hasNext(); ) {
                    short curTarget = iter.next();
                    short curWeight = pToWeight.get(curTarget);
                    if(curWeight < minWeightForPartition) {
                        minWeightForPartition = curWeight;
                        targetPid = curTarget;
                    }
                }
            }

            return new Target(id, targetPid, pid, (float) maxGain);
        }

        public float getImbalanceFactor(short pId, short offset) {
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
            if (id !=(that.id)) return false;
            if (pid != that.pid) return false;
            if (pToFriendCount != that.pToFriendCount) return false;
            if (pToWeight != that.pToWeight) return false;
            return totalWeight == that.totalWeight;

        }

        @Override
        public int hashCode() {
            int result = id;
            result = 31 * result + pid;
            result = 31 * result + (gamma != +0.0f ? Float.floatToIntBits(gamma) : 0);
            result = 31 * result + pToFriendCount.hashCode();
            result = 31 * result + pToWeight.hashCode();
            result = 31 * result + totalWeight;
            return result;
        }

        @Override
        public String toString() {
            return "u" + id + "|, p" + pid + "|" + pToFriendCount;
        }
    }

}
