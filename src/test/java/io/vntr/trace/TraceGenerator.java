package io.vntr.trace;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.list.TShortList;
import gnu.trove.list.array.TShortArrayList;
import gnu.trove.list.linked.TShortLinkedList;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.TestUtils;
import io.vntr.utils.ProbabilityUtils;
import io.vntr.utils.TroveUtils;

import java.util.*;

import static io.vntr.TestUtils.*;
import static io.vntr.utils.TroveUtils.*;
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
    private static final short USERS_PER_PARTITION = 150;
    private static final short MIN_NUM_REPLICAS = 0;
    private static final double PROB_RANDOM_FRIENDSHIP = 0.2;

    private static short maxUid = 0;
    private static short maxPid = 0;

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
        TShortObjectMap<TShortSet> mutableFriendships = TestUtils.extractFriendshipsFromFile(filename);
        TShortObjectMap<TShortSet> friendships = copyTShortObjectMapIntSet(mutableFriendships);

        int numPids = 1 + (mutableFriendships.size() / USERS_PER_PARTITION);
        if(numPids < 3 + MIN_NUM_REPLICAS) {
            numPids = 3 + MIN_NUM_REPLICAS;
        }
        TShortSet pids = new TShortHashSet(numPids+1);
        for (short pid = 0; pid < numPids; pid++) {
            pids.add(pid);
        }

        TShortObjectMap<TShortSet> partitions = TestUtils.getRandomPartitioning(pids, mutableFriendships.keySet());
        TShortObjectMap<TShortSet> replicas = TroveUtils.getInitialReplicasObeyingKReplication(MIN_NUM_REPLICAS, partitions, mutableFriendships);

        TRACE_ACTION[] script = new TRACE_ACTION[numActions];
        for (int j = 0; j < numActions - 1; j++) {
            script[j] = getActions(actionsProbability);
        }
        script[numActions - 1] = DOWNTIME;

        maxPid = (short) pids.size();
        maxUid = (short) mutableFriendships.size();

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

    static void printStatistics(int numP, TShortObjectMap<TShortSet> friendships) {
        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);

        int numU = friendships.size();
        int numF = 0;
        NavigableSet<Integer> numFriends = new TreeSet<>();
        for(short uid : friendships.keys()) {
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

    static FullTraceAction addU(TShortObjectMap<TShortSet> friendships, TShortSet pids) {
        short newUid = ++maxUid;
        friendships.put(newUid, new TShortHashSet());

        return new FullTraceAction(ADD_USER, newUid);
    }

    static FullTraceAction cutU(TShortObjectMap<TShortSet> friendships, TShortSet pids) {
        short userToRemove = getRandomElement(friendships.keySet());

        for(TShortIterator iter = findKeysForUser(friendships, userToRemove).iterator(); iter.hasNext(); ) {
            friendships.get(iter.next()).remove(userToRemove);
        }

        friendships.remove(userToRemove);
        return new FullTraceAction(REMOVE_USER, userToRemove);
    }

    static FullTraceAction addF(TShortObjectMap<TShortSet> friendships, TShortSet pids) {
        TShortObjectMap<TShortSet> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        short uid = chooseKeyFromMapSetInProportionToSetSize(bidirectionalFriendships);
        TShortList friendIds = new TShortArrayList(bidirectionalFriendships.get(uid));

        //Grab a new friend either uniformly from friends of friends, or at random from everyone this user hasn't befriended
        if(Math.random() > PROB_RANDOM_FRIENDSHIP) {
            TShortList friendsOfFriends = new TShortLinkedList();
            for(int i=0; i<friendIds.size(); i++) {
                //We want all friends of this friend, except people who are already friends with uid
                TShortSet friendsOfThisFriend = new TShortHashSet(friendships.get(friendIds.get(i)));
                friendsOfThisFriend.addAll(findKeysForUser(friendships, uid));
                friendsOfThisFriend.removeAll(friendIds);
                friendsOfThisFriend.remove(uid);
                friendsOfFriends.addAll(convert(friendsOfThisFriend));
            }
            if(!friendsOfFriends.isEmpty()) {
                int friendId = friendsOfFriends.get((int) (friendsOfFriends.size() * Math.random()));
                return innerBefriend(uid, friendId, friendships);
            }
        }

        //TODO: figure out how to select a nonFriendId with a preference towards those with a similar number of friends
        TShortSet nonFriendIds = new TShortHashSet(friendships.keySet());
        nonFriendIds.removeAll(friendIds);
        nonFriendIds.remove(uid);

        if(nonFriendIds.isEmpty()) {
            throw new RuntimeException("User " + uid + " is friends with everyone!");
        }

        TShortObjectMap<TShortSet> bidirectionalNonFriends = copyTShortObjectMapIntSet(bidirectionalFriendships);
        bidirectionalNonFriends.keySet().retainAll(nonFriendIds);

        int friendId = chooseKeyFromMapSetWithCorrelationToSizeOfNode(bidirectionalNonFriends, bidirectionalFriendships.get(uid).size());

        return innerBefriend(uid, friendId, friendships);
    }

    private static FullTraceAction innerBefriend(int oneUid, int theOtherUid, TShortObjectMap<TShortSet> friendships) {
        short val1 = (short) Math.min(oneUid, theOtherUid);
        short val2 = (short) Math.max(oneUid, theOtherUid);
        friendships.get(val1).add(val2);
        return new FullTraceAction(BEFRIEND, val1, val2);
    }

    static FullTraceAction cutF(TShortObjectMap<TShortSet> friendships, TShortSet pids) {
        List<Short> friendship = chooseKeyValuePairFromMapSetUniformly(friendships);
        short val1 = (short) Math.min(friendship.get(0), friendship.get(1));
        short val2 = (short) Math.max(friendship.get(0), friendship.get(1));
        friendships.get(val1).remove(val2);

        return new FullTraceAction(UNFRIEND, val1, val2);
    }

    static FullTraceAction addP(TShortObjectMap<TShortSet> friendships, TShortSet pids) {
        short newPid = ++maxPid;
        pids.add(newPid);
        return new FullTraceAction(ADD_PARTITION, newPid);
    }

    static FullTraceAction cutP(TShortObjectMap<TShortSet> friendships, TShortSet pids) {
        short partitionToRemove = getRandomElement(pids);
        pids.remove(partitionToRemove);

        return new FullTraceAction(REMOVE_PARTITION, partitionToRemove);
    }

    static int chooseKeyFromMapSetWithCorrelationToSizeOfNode(TShortObjectMap<TShortSet> mapset, int numFriendsOfUser) {
        double numFriendsOfUserDouble = (double) numFriendsOfUser;
        TShortList keys = new TShortArrayList(mapset.size() * 100);
        for(short key : mapset.keys()) {
            int numEntries = getNumEntries((double) mapset.get(key).size(), numFriendsOfUserDouble);
            for(short i=0; i<numEntries; i++) {
                keys.add(key); //this is intentional: we want numEntries copies of key in there
            }
        }

        return keys.get((int) (Math.random() * keys.size()));
    }

    static int getNumEntries(double thisSize, double comparisonSize) {
        double lesser  = Math.min(thisSize, comparisonSize);
        double greater = Math.max(thisSize, comparisonSize);
        double similarity = Math.pow(lesser/greater, 2);
        return 1 + (int) (9 * similarity);
    }

}
