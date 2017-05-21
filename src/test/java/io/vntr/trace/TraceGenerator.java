package io.vntr.trace;

import io.vntr.TestUtils;
import io.vntr.utils.ProbabilityUtils;

import java.util.*;

import static io.vntr.TestUtils.*;
import static io.vntr.utils.Utils.generateBidirectionalFriendshipSet;
import static io.vntr.trace.TRACE_ACTION.*;
import static io.vntr.utils.ProbabilityUtils.*;

/**
 * Created by robertlindquist on 10/29/16.
 */
public class TraceGenerator {
    private static final String LESKOVEC_FACEBOOK_FILENAME = "/Users/robertlindquist/Documents/thesis/data/leskovec_facebook/leskovec-facebook.txt";
    private static final String MISLOVE_FACEBOOK_FILENAME  = "/Users/robertlindquist/Documents/thesis/data/mislove_wson_2009_facebook/facebook-links-reparsed-02.txt";
    private static final String ZACHARY_KARATE_FILENAME    = "/Users/robertlindquist/Documents/thesis/data/zachary_karate/karate.txt";
    private static final String SYNTHETIC_2K_FILENAME      = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u2000_f22723_g18_gp_0_19_fp_0_017_asrt_-0_00001470_20170119181335.txt";
    private static final String SYNTHETIC_2K_FILENAMEx2    = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u2000_f22155_g18_gp_0_19_fp_0_017_asrt_-0_00001319_20170119193734.txt";
    private static final String SYNTHETIC_3K_FILENAME      = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u3000_f51487_g20_gp_0_15_fp_0_025_asrt_-0_00000039_20170119181019.txt";
    private static final String SYNTHETIC_3K_FILENAMEx2    = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u3000_f49864_g20_gp_0_15_fp_0_025_asrt_0_00000083_20170119194131.txt";
    private static final String SYNTHETIC_70K_FILENAME     = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u70000_f984874_g1500_gp_0_0022_fp_0_055_20170518150632.txt";



    private static final String OUTPUT_DIR = "/Users/robertlindquist/Documents/thesis/traces/";

    private static final int NUM_ACTIONS = 100001;
    private static final int USERS_PER_PARTITION = 150;
    private static final int MIN_NUM_REPLICAS = 0;
    private static final double PROB_RANDOM_FRIENDSHIP = 0.2;

    private static int maxUid = 0;
    private static int maxPid = 0;

    private static final Map<TRACE_ACTION, Double> actionsProbability = new HashMap<>();
    static {
        actionsProbability.put(ADD_USER, 0.1D);
        actionsProbability.put(REMOVE_USER, 0.01D);
        actionsProbability.put(BEFRIEND, 0.66D); //Befriend/Unfriend ratio should be ~3, according to sigmod-linkbench paper
        actionsProbability.put(UNFRIEND, 0.22D);
        actionsProbability.put(ADD_PARTITION, 0D);  //this should be triggered by adding users or removing partitions...
        actionsProbability.put(REMOVE_PARTITION, 0.005D);
        actionsProbability.put(DOWNTIME, 0.005D);
    }

    private static final String INPUT_FILE = LESKOVEC_FACEBOOK_FILENAME;
    private static final String OUTPUT_FILENAME_STUB = OUTPUT_DIR + "facebook_super_long_0_";

    public static void main(String[] args) throws Exception {
        BaseTrace baseTrace = generateTrace(INPUT_FILE, NUM_ACTIONS);
        TraceTestUtils.writeTraceToFile(OUTPUT_FILENAME_STUB + "bootstrapped_" + System.nanoTime() + ".txt", baseTrace);
    }

    private static BaseTrace generateTrace(String filename, int numActions) throws Exception {
        Map<Integer, Set<Integer>> mutableFriendships = TestUtils.extractFriendshipsFromFile(filename);
        Map<Integer, Set<Integer>> friendships = copyMapSet(mutableFriendships);

        int numPids = 1 + (mutableFriendships.size() / USERS_PER_PARTITION);
        if(numPids < 3 + MIN_NUM_REPLICAS) {
            numPids = 3 + MIN_NUM_REPLICAS;
        }
        Set<Integer> pids = new HashSet<>();
        for (int pid = 0; pid < numPids; pid++) {
            pids.add(pid);
        }

        Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, mutableFriendships.keySet());
        Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(MIN_NUM_REPLICAS, partitions, mutableFriendships);

        TRACE_ACTION[] script = new TRACE_ACTION[numActions];
        for (int j = 0; j < numActions - 1; j++) {
            script[j] = getActions(actionsProbability);
        }
        script[numActions - 1] = DOWNTIME;

        maxPid = pids.size();
        maxUid = mutableFriendships.size();

        System.out.print("Starting statistics: ");
        printStatistics(pids.size(), mutableFriendships);

        List<FullTraceAction> actions = new LinkedList<>();
        for(int i=0; i<numActions; i++) {
            switch (script[i]) {
                case ADD_USER:         actions.add(addU(mutableFriendships, pids)); break;
                case REMOVE_USER:      actions.add(cutU(mutableFriendships, pids)); break;
                case BEFRIEND:         actions.add(addF(mutableFriendships, pids)); break;
                case UNFRIEND:         actions.add(cutF(mutableFriendships, pids)); break;
                case REMOVE_PARTITION: actions.add(cutP(mutableFriendships, pids)); break;
                case DOWNTIME:         actions.add(new FullTraceAction(DOWNTIME));  break;
            }

            if(i != 0 && i%100 == 0) {
                System.out.printf("Stats for i=%7d: ", i);
                printStatistics(pids.size(), mutableFriendships);
            }

            double usersPerPartition = ((double) mutableFriendships.size()) / pids.size();
            if(usersPerPartition > USERS_PER_PARTITION) {
                actions.add(addP(mutableFriendships, pids));
            }
        }

        System.out.print("Ending statistics:   ");
        printStatistics(pids.size(), mutableFriendships);


        return new TraceWithReplicas(friendships, partitions, replicas, actions);
    }

    static void printStatistics(int numP, Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        int numU = friendships.size();
        int numF = 0;
        NavigableSet<Integer> numFriends = new TreeSet<>();
        for(int uid : friendships.keySet()) {
            numF += friendships.get(uid).size();
            numFriends.add(bidirectionalFriendships.get(uid).size());
        }
        double assortivity = ProbabilityUtils.calculateAssortivityCoefficient(friendships);

        Iterator<Integer> descNumFriendsIter = numFriends.descendingIterator();
        int highestNumFriendships = descNumFriendsIter.next();
        int secondHighestNumFriendships = descNumFriendsIter.next();
        int thirdHighestNumFriendships = descNumFriendsIter.next();

        String statisticsPrintFormat = "#P: %3d, #U: %5d, #F: %6d, Assortivity: %1.8f, top 3 friendships: (%4d, %4d, %4d)%n";

        System.out.printf(statisticsPrintFormat, numP, numU, numF, assortivity, highestNumFriendships, secondHighestNumFriendships, thirdHighestNumFriendships);
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

    static FullTraceAction addU(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int newUid = ++maxUid;
        friendships.put(newUid, new HashSet<Integer>());

        return new FullTraceAction(ADD_USER, newUid);
    }

    static FullTraceAction cutU(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int userToRemove = getRandomElement(friendships.keySet());

        for(int friendId : findKeysForUser(friendships, userToRemove)) {
            friendships.get(friendId).remove(userToRemove);
        }

        friendships.remove(userToRemove);
        return new FullTraceAction(REMOVE_USER, userToRemove);
    }

    static FullTraceAction addF(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        int uid = chooseKeyFromMapSetInProportionToSetSize(bidirectionalFriendships);
        List<Integer> friendIds = new LinkedList<>(bidirectionalFriendships.get(uid));

        //Grab a new friend either uniformly from friends of friends, or at random from everyone this user hasn't befriended
        if(Math.random() > PROB_RANDOM_FRIENDSHIP) {
            List<Integer> friendsOfFriends = new LinkedList<>();
            for(int friendId : friendIds) {
                //We want all friends of this friend, except people who are already friends with uid
                Set<Integer> friendsOfThisFriend = new HashSet<>(friendships.get(friendId));
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

        //TODO: figure out how to select a nonFriendId with a preference towards those with a similar number of friends
        Set<Integer> nonFriendIds = new HashSet<>(friendships.keySet());
        nonFriendIds.removeAll(friendIds);
        nonFriendIds.remove(uid);

        if(nonFriendIds.isEmpty()) {
            throw new RuntimeException("User " + uid + " is friends with everyone!");
        }

        Map<Integer, Set<Integer>> bidirectionalNonFriends = copyMapSet(bidirectionalFriendships);
        bidirectionalNonFriends.keySet().retainAll(nonFriendIds);

        int friendId = chooseKeyFromMapSetWithCorrelationToSizeOfNode(bidirectionalNonFriends, bidirectionalFriendships.get(uid).size());

        return innerBefriend(uid, friendId, friendships);
    }

    private static FullTraceAction innerBefriend(int oneUid, int theOtherUid, Map<Integer, Set<Integer>> friendships) {
        int val1 = Math.min(oneUid, theOtherUid);
        int val2 = Math.max(oneUid, theOtherUid);
        friendships.get(val1).add(val2);
        return new FullTraceAction(BEFRIEND, val1, val2);
    }

    static FullTraceAction cutF(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        List<Integer> friendship = chooseKeyValuePairFromMapSetUniformly(friendships);
        int val1 = Math.min(friendship.get(0), friendship.get(1));
        int val2 = Math.max(friendship.get(0), friendship.get(1));
        friendships.get(val1).remove(val2);

        return new FullTraceAction(UNFRIEND, val1, val2);
    }

    static FullTraceAction addP(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int newPid = ++maxPid;
        pids.add(newPid);
        return new FullTraceAction(ADD_PARTITION, newPid);
    }

    static FullTraceAction cutP(Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int partitionToRemove = getRandomElement(pids);
        pids.remove(partitionToRemove);

        return new FullTraceAction(REMOVE_PARTITION, partitionToRemove);
    }

    static int chooseKeyFromMapSetWithCorrelationToSizeOfNode(Map<Integer, Set<Integer>> mapset, int numFriendsOfUser) {
        double numFriendsOfUserDouble = (double) numFriendsOfUser;
        List<Integer> keys = new ArrayList<>(mapset.size() * 100);
        for(int key : mapset.keySet()) {
            int numEntries = getNumEntries((double) mapset.get(key).size(), numFriendsOfUserDouble);
            for(int i=0; i<numEntries; i++) {
                keys.add(key); //this is intentional: we want numEntries copies of key in there
            }
        }

        return ProbabilityUtils.getRandomElement(keys);
    }

    static int getNumEntries(double thisSize, double comparisonSize) {
        double lesser  = Math.min(thisSize, comparisonSize);
        double greater = Math.max(thisSize, comparisonSize);
        double similarity = Math.pow(lesser/greater, 2);
        return 1 + (int) (9 * similarity);
    }

}
