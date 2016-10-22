package io.vntr.trace;

import io.vntr.TestUtils;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.TestUtils.copyMapSet;
import static io.vntr.TestUtils.findKeysForUser;
import static io.vntr.TestUtils.getTopographyForMultigroupSocialNetwork;
import static io.vntr.trace.TRACE_ACTION.*;
import static io.vntr.trace.TRACE_ACTION.DOWNTIME;
import static io.vntr.utils.ProbabilityUtils.*;

/**
 * Created by robertlindquist on 10/21/16.
 */
public class TraceGenerator {

    public static void main(String[] args) throws Exception {
        Trace trace = generateTrace(10001);
        TraceUtils.writeTraceToFile("/Users/robertlindquist/Documents/enhanced_trace_" + System.nanoTime() + ".txt", trace);
    }

    static Trace generateTrace(int size) {

        int numUsers = 500 + (int) (Math.random() * 2000);
        int numGroups = 6 + (int) (Math.random() * 20);
        float groupProb = 0.03f + (float) (Math.random() * 0.1);
        float friendProb = 0.03f + (float) (Math.random() * 0.1);
        Map<Integer, Set<Integer>> mutableFriendships = getTopographyForMultigroupSocialNetwork(numUsers, numGroups, groupProb, friendProb);

        int usersPerPartition = 50 + (int) (Math.random() * 100);

        Set<Integer> pids = new HashSet<Integer>();
        for (int pid = 0; pid < mutableFriendships.size() / usersPerPartition; pid++) {
            pids.add(pid);
        }

        Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, mutableFriendships.keySet());
        Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(2, partitions, mutableFriendships);

        Map<Integer, Set<Integer>> friendships = copyMapSet(mutableFriendships);

        Map<TRACE_ACTION, Double> actionsProbability = new HashMap<TRACE_ACTION, Double>();
        actionsProbability.put(ADD_USER, 0.15D);
        actionsProbability.put(REMOVE_USER, 0.05D);
        actionsProbability.put(BEFRIEND, 0.665D);
        actionsProbability.put(UNFRIEND, 0.05D);
        actionsProbability.put(ADD_PARTITION, 0.05D);
        actionsProbability.put(REMOVE_PARTITION, 0.01D);
        actionsProbability.put(DOWNTIME, 0.025D);

        TRACE_ACTION[] script = new TRACE_ACTION[size];
        for (int j = 0; j < size - 1; j++) {
            script[j] = getActions(actionsProbability);
        }
        script[size - 1] = DOWNTIME;

        List<FullTraceAction> actions = new LinkedList<FullTraceAction>();
        for(int i=0; i<size; i++) {
            switch (script[i]) {
                case ADD_USER:         actions.add(handleAddUser        (mutableFriendships, pids)); break;
                case REMOVE_USER:      actions.add(handleRemoveUser     (mutableFriendships, pids)); break;
                case BEFRIEND:         actions.add(handleBefriend       (mutableFriendships, pids)); break;
                case UNFRIEND:         actions.add(handleUnfriend       (mutableFriendships, pids)); break;
                case ADD_PARTITION:    actions.add(handleAddPartition   (mutableFriendships, pids)); break;
                case REMOVE_PARTITION: actions.add(handleRemovePartition(mutableFriendships, pids)); break;
                case DOWNTIME:         actions.add(new FullTraceAction(DOWNTIME));                        break;
            }
        }

        return new TraceWithReplicas(friendships, partitions, replicas, actions);
    }

    static FullTraceAction handleAddUser(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int newUid = new TreeSet<Integer>(friendships.keySet()).last()+1;
        System.out.println("Adding u" + newUid);
        friendships.put(newUid, new HashSet<Integer>());
        return new FullTraceAction(ADD_USER, newUid);
    }

    static FullTraceAction handleRemoveUser(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int userToRemove = getRandomElement(friendships.keySet());

        for(int friendId : findKeysForUser(friendships, userToRemove)) {
            friendships.get(friendId).remove(userToRemove);
        }

        System.out.println("Removing u" + userToRemove);
        friendships.remove(userToRemove);
        return new FullTraceAction(REMOVE_USER, userToRemove);
    }

    static FullTraceAction handleBefriend(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int uid = chooseKeyFromMapSetInProportionToSetSize(generateBidirectionalFriendshipSet(friendships));
        List<Integer> friendIds = new LinkedList<Integer>(friendships.get(uid));
        friendIds.addAll(findKeysForUser(friendships, uid));

        //Grab a new friend either uniformly from friends of friends, or at random from everyone this user hasn't befriended
        if(Math.random() > 0.5) {
            List<Integer> friendsOfFriends = new LinkedList<Integer>();
            for(int friendId : friendIds) {
                //We want all friends of this friend, except people who are already friends with uid
                Set<Integer> friendsOfThisFriend = new HashSet<Integer>(friendships.get(friendId));
                friendsOfThisFriend.addAll(findKeysForUser(friendships, uid));
                friendsOfThisFriend.removeAll(friendIds);
                friendsOfThisFriend.remove(uid);
                friendsOfFriends.addAll(friendsOfThisFriend);
            }
            if(!friendsOfFriends.isEmpty()) {
                int friendId = ProbabilityUtils.getRandomElement(friendsOfFriends);
                return innerBefriend(uid, friendId, friendships);
            }
        }

        Set<Integer> nonFriendIds = new HashSet<Integer>(friendships.keySet());
        nonFriendIds.removeAll(friendIds);
        nonFriendIds.remove(uid);

        if(nonFriendIds.isEmpty()) {
            throw new RuntimeException("User " + uid + " is friends with everyone!");
        }

        int friendId = ProbabilityUtils.getRandomElement(nonFriendIds);
        return innerBefriend(uid, friendId, friendships);
    }

    private static FullTraceAction innerBefriend(int oneUid, int theOtherUid, Map<Integer, Set<Integer>> friendships) {
        int val1 = Math.min(oneUid, theOtherUid);
        int val2 = Math.max(oneUid, theOtherUid);
        System.out.println("Befriending " + val1 + " and " + val2);
        friendships.get(val1).add(val2);
        return new FullTraceAction(BEFRIEND, val1, val2);
    }

    static FullTraceAction handleUnfriend(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        List<Integer> friendship = chooseKeyValuePairFromMapSetUniformly(friendships);
        int val1 = Math.min(friendship.get(0), friendship.get(1));
        int val2 = Math.max(friendship.get(0), friendship.get(1));
        friendships.get(val1).remove(val2);

        System.out.println("Unfriending " + val1 + " and " + val2);

        return new FullTraceAction(UNFRIEND, val1, val2);
    }

    static FullTraceAction handleAddPartition(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int newPid = new TreeSet<Integer>(pids).last()+1;
        System.out.println("Adding p" + newPid);
        pids.add(newPid);
        return new FullTraceAction(ADD_PARTITION, newPid);
    }

    static FullTraceAction handleRemovePartition(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int partitionToRemove = getRandomElement(pids);
        System.out.println("Removing p" + partitionToRemove);
        pids.remove(partitionToRemove);
        return new FullTraceAction(REMOVE_PARTITION, partitionToRemove);
    }

    static TRACE_ACTION getActions(Map<TRACE_ACTION, Double> actionsProbability) {
        double random = Math.random();

        for(TRACE_ACTION TRACEAction : TRACE_ACTION.values()) {
            if(random < actionsProbability.get(TRACEAction)) {
                return TRACEAction;
            }
            random -= actionsProbability.get(TRACEAction);
        }

        return DOWNTIME;
    }

}
