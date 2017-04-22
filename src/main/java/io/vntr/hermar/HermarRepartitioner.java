package io.vntr.hermar;

import io.vntr.RepUser;
import io.vntr.User;

import java.util.*;

/**
 * Created by robertlindquist on 4/21/17.
 */
public class HermarRepartitioner {

    private HermarManager manager;

    public HermarRepartitioner(HermarManager manager) {
        this.manager = manager;
    }

    public void repartition(int k, int maxIterations) {
        int moves = 0;
        State state = initState();
        for(int i=0; i<maxIterations; i++) {
            int movesBeforeIteration = moves;

            moves += performStage(true, k, state);
            state.setLogicalUsers(initLogicalUsers());

            moves += performStage(false, k, state);
            state.setLogicalUsers(initLogicalUsers());

            if(moves == movesBeforeIteration) {
                break;
            }
        }

        if(moves > 0) {
            physicallyMigrate(state.getLogicalPartitions());
        }

        manager.increaseTallyLogical(moves);
    }

    static int performStage(boolean firstStage, int k, State state) {
        Set<Target> targets = new HashSet<>();
        for(int pid : state.getLogicalPartitions().keySet()) {
            targets.addAll(getCandidates(pid, firstStage, k, state));
        }

        logicallyMigrate(targets, state);

        return targets.size();
    }

    static Set<Target> getCandidates(int pid, boolean firstIteration, int k, State state) {
        NavigableSet<Target> candidates = new TreeSet<>();
        for(Integer uid : state.getLogicalPartitions().get(pid)) {
            Target target = state.getLogicalUsers().get(uid).getTargetPart(firstIteration);
            if(target.partitionId != null) {
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
            state.getLogicalPartitions().get(target.oldPartitionId).remove(target.userId);
            state.getLogicalPartitions().get(target.partitionId).add(target.userId);
        }
    }

    void physicallyMigrate(Map<Integer, Set<Integer>> logicalPartitions) {
        for(int pid : logicalPartitions.keySet()) {
            for(int uid : logicalPartitions.get(pid)) {
                User user = manager.getUser(uid);
                if(user.getBasePid() != pid) {
                    manager.moveUser(uid, pid);
                    manager.increaseTally(1);
                }
            }
        }
    }

    Map<Integer, LogicalUser> initLogicalUsers() {
        Map<Integer, Integer> pToWeight = new HashMap<>();
        int totalWeight = 0;
        float gamma = manager.getGamma();

        for(int pid : manager.getAllPartitionIds()) {
            Set<Integer> uids = manager.getPartitionById(pid);
            pToWeight.put(pid, uids.size());
            totalWeight += uids.size();
        }

        Map<Integer, LogicalUser> logicalUsers = new HashMap<>();
        for(int uid : manager.getUserIds()) {
            User user = manager.getUser(uid);
            Map<Integer, Integer> pToFriendCount = new HashMap<>();
            for(int pid : manager.getAllPartitionIds()) {
                pToFriendCount.put(pid, 0);
            }
            for(Integer friendId : user.getFriendIDs()) {
                int pid = manager.getPartitionIdForUser(friendId);
                pToFriendCount.put(pid, pToFriendCount.get(pid) + 1);
            }

            int pid = manager.getPartitionIdForUser(uid);
            logicalUsers.put(uid, new LogicalUser(uid, pid, gamma, pToFriendCount, new HashMap<>(pToWeight), totalWeight));
        }

        return logicalUsers;
    }

    State initState() {
        State state = new State();
        Map<Integer, Set<Integer>> logicalPartitions = new HashMap<>();

        for(int pid : manager.getAllPartitionIds()) {
            Set<Integer> uids = manager.getPartitionById(pid);
            logicalPartitions.put(pid, new HashSet<>(uids));
        }

        state.setLogicalPartitions(logicalPartitions);
        state.setLogicalUsers(initLogicalUsers());

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

        public void setLogicalUsers(Map<Integer, LogicalUser> logicalUsers) {
            this.logicalUsers = logicalUsers;
        }
    }

    static class LogicalUser {
        private Integer id;
        private Integer pid;
        private float gamma;
        private Map<Integer, Integer> pToFriendCount;
        private Map<Integer, Integer> pToWeight;
        private Integer totalWeight;

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

        public void setpToWeight(Map<Integer, Integer> pToWeight) {
            this.pToWeight = pToWeight;
        }

        public Integer getTotalWeight() {
            return totalWeight;
        }

        public void setTotalWeight(Integer totalWeight) {
            this.totalWeight = totalWeight;
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
                return new Target(id, null, null, 0);
            }

            Set<Integer> targets = new HashSet<>();
            Integer maxGain = overweight ? Integer.MIN_VALUE : 0;

            for(Integer targetPid : pToFriendCount.keySet()) {

                if((firstStage && targetPid > pid) || (!firstStage && targetPid < pid)) {

                    try {
                        int gain = pToFriendCount.get(targetPid) - pToFriendCount.get(pid);
                        float balanceFactor = getImbalanceFactor(targetPid, 1);

                        if (gain > maxGain && balanceFactor < gamma) {
                            targets = new HashSet<>();
                            targets.add(targetPid);
                            maxGain = gain;
                        } else if (gain == maxGain && (overweight || gain > 0)) {
                            targets.add(targetPid);
                        }
                    } catch(Exception e) {
                        System.out.println();
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

            return new Target(id, targetPid, pid, maxGain);
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
