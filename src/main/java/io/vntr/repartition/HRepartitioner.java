package io.vntr.repartition;

import java.util.*;

import static io.vntr.utils.Utils.getUToMasterMap;
import static io.vntr.utils.Utils.getUserCounts;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class HRepartitioner {
    public static NoRepResults repartition(int k, int maxIterations, float gamma, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships) {
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
        for(int pid : state.getLogicalPartitions().keySet()) {
            targets.addAll(getCandidates(pid, firstStage, k, state));
        }

        logicallyMigrate(targets, state);

        return targets;
    }

    static Set<Target> getCandidates(int pid, boolean firstIteration, int k, State state) {
        NavigableSet<Target> candidates = new TreeSet<>();
        for(Integer uid : state.getLogicalPartitions().get(pid)) {
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

    static State initState(Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> friendships, float gamma) {
        State state = new State();
        Map<Integer, Set<Integer>> logicalPartitions = new HashMap<>();

        for(int pid : partitions.keySet()) {
            Set<Integer> uids = partitions.get(pid);
            logicalPartitions.put(pid, new HashSet<>(uids));
        }

        state.setLogicalPartitions(logicalPartitions);
        state.updateLogicalUsers(friendships, gamma);

        return state;
    }

    static class State {

        private Map<Integer, Set<Integer>> logicalPartitions;
        private Map<Integer, LogicalUser> logicalUsers;
        public State() {
        }

        public Map<Integer, Set<Integer>> getLogicalPartitions() {
            return logicalPartitions;
        }

        public void setLogicalPartitions(Map<Integer, Set<Integer>> logicalPartitions) {
            this.logicalPartitions = logicalPartitions;
        }

        public Map<Integer, LogicalUser> getLogicalUsers() {
            return logicalUsers;
        }

        public void updateLogicalUsers(Map<Integer, Set<Integer>> friendships, float gamma) {
            setLogicalUsers(initLogicalUsers(logicalPartitions, friendships, gamma));
        }

        public void updateLogicalUsers(Set<Target> targets, Map<Integer, Set<Integer>> friendships) {
            Map<Integer, Integer> pToWeight = getUserCounts(logicalPartitions);
            for(LogicalUser user : logicalUsers.values()) {
                user.pToWeight = new HashMap<>(pToWeight);
            }
            for(Target target : targets) {
                getLogicalUsers().get(target.uid).pid = target.pid;
                for(int friendId : friendships.get(target.uid)) {
                    Map<Integer, Integer> pToFriendCount = logicalUsers.get(friendId).pToFriendCount;
                    pToFriendCount.put(target.oldPid, pToFriendCount.get(target.oldPid) - 1);
                    pToFriendCount.put(target.pid, pToFriendCount.get(target.pid) + 1);
                }
            }
        }

        public void setLogicalUsers(Map<Integer, LogicalUser> logicalUsers) {
            this.logicalUsers = logicalUsers;
        }

        static Map<Integer, LogicalUser> initLogicalUsers(Map<Integer, Set<Integer>> logicalPids, Map<Integer, Set<Integer>> friendships, float gamma) {
            Map<Integer, Integer> uidToPidMap = getUToMasterMap(logicalPids);

            Map<Integer, Integer> pToWeight = new HashMap<>();
            int totalWeight = 0;
            for(int pid : logicalPids.keySet()) {
                int numUsersOnPartition = logicalPids.get(pid).size();
                pToWeight.put(pid, numUsersOnPartition);
                totalWeight += numUsersOnPartition;
            }

            Map<Integer, LogicalUser> logicalUsers = new HashMap<>();
            for(Set<Integer> partition : logicalPids.values()) {
                for(int uid : partition) {
                    Map<Integer, Integer> pToFriendCount = new HashMap<>();
                    for(int pid : logicalPids.keySet()) {
                        pToFriendCount.put(pid, 0);
                    }
                    for(Integer friendId : friendships.get(uid)) {
                        int pid = uidToPidMap.get(friendId);
                        pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
                    }

                    int pid = uidToPidMap.get(uid);
                    logicalUsers.put(uid, new LogicalUser(uid, pid, gamma, pToFriendCount, new HashMap<>(pToWeight), totalWeight));
                }
            }

            return logicalUsers;
        }
    }

    static class LogicalUser {
        private final Integer id;
        private Integer pid;
        private float gamma;
        private Map<Integer, Integer> pToFriendCount;
        private Map<Integer, Integer> pToWeight;
        private final Integer totalWeight;

        public LogicalUser(Integer id, Integer pid, float gamma, Map<Integer, Integer> pToFriendCount, Map<Integer, Integer> pToWeight, Integer totalWeight) {
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

        public Map<Integer, Integer> getpToWeight() {
            return pToWeight;
        }

        public Integer getTotalWeight() {
            return totalWeight;
        }

        public Integer getPid() {
            return pid;
        }

        public Map<Integer, Integer> getpToFriendCount() {
            return pToFriendCount;
        }

        public Target getTargetPart(boolean firstStage) {
            boolean underweight = getImbalanceFactor(pid, -1) < (2-gamma);
            boolean overweight = getImbalanceFactor(pid, 0) > gamma;

            if(underweight) {
                return new Target(id, null, null, 0f);
            }

            Set<Integer> targets = new HashSet<>();
            Integer maxGain = overweight ? Integer.MIN_VALUE : 0;

            for(Integer targetPid : pToFriendCount.keySet()) {

                if((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) {

                    int gain = pToFriendCount.get(targetPid) - pToFriendCount.get(pid);
                    float balanceFactor = getImbalanceFactor(targetPid, 1);

                    if (gain > maxGain && balanceFactor < gamma) {
                        targets = new HashSet<>();
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
                for(int curTarget : targets) {
                    int curWeight = pToWeight.get(curTarget);
                    if(curWeight < minWeightForPartition) {
                        minWeightForPartition = curWeight;
                        targetPid = curTarget;
                    }
                }
            }

            return new Target(id, targetPid, pid, (float) maxGain);
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
