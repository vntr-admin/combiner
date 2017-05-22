package io.vntr.trace;

import io.vntr.TestUtils;
import io.vntr.repartition.MetisRepartitioner;
import io.vntr.utils.ProbabilityUtils;

import java.io.FileInputStream;
import java.util.*;

import static io.vntr.TestUtils.copyMapSet;
import static io.vntr.TestUtils.findKeysForUser;
import static io.vntr.trace.TRACE_ACTION.*;
import static io.vntr.utils.ProbabilityUtils.chooseKeyFromMapSetInProportionToSetSize;
import static io.vntr.utils.ProbabilityUtils.chooseKeyValuePairFromMapSetUniformly;
import static io.vntr.utils.ProbabilityUtils.getRandomElement;
import static io.vntr.utils.Utils.generateBidirectionalFriendshipSet;
import static java.util.Collections.nCopies;

/**
 * Created by robertlindquist on 5/7/17.
 */
public class WeeklyTraceSetGenerator {
    private static final String LESKOVEC_FACEBOOK_FILENAME = "/Users/robertlindquist/Documents/thesis/data/leskovec_facebook/leskovec-facebook.txt";
    private static final String MISLOVE_FACEBOOK_FILENAME  = "/Users/robertlindquist/Documents/thesis/data/mislove_wson_2009_facebook/facebook-links-reparsed-02.txt";
    private static final String ASU_FRIENDSTER_FILENAME    = "/Users/robertlindquist/Documents/thesis/data/asu_friendster/asu-friendster.txt";
    private static final String ZACHARY_KARATE_FILENAME    = "/Users/robertlindquist/Documents/thesis/data/zachary_karate/karate.txt";
    private static final String SYNTHETIC_2K_FILENAME      = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u2000_f22723_g18_gp_0_19_fp_0_017_asrt_-0_00001470_20170119181335.txt";
    private static final String SYNTHETIC_2K_FILENAMEx2    = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u2000_f22155_g18_gp_0_19_fp_0_017_asrt_-0_00001319_20170119193734.txt";
    private static final String SYNTHETIC_3K_FILENAME      = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u3000_f51487_g20_gp_0_15_fp_0_025_asrt_-0_00000039_20170119181019.txt";
    private static final String SYNTHETIC_3K_FILENAMEx2    = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u3000_f49864_g20_gp_0_15_fp_0_025_asrt_0_00000083_20170119194131.txt";
    private static final String SYNTHETIC_70K_FILENAME     = "/Users/robertlindquist/Documents/thesis/data/synthetic/synth_u70000_f984874_g1500_gp_0_0022_fp_0_055_20170518150632.txt";

    // from the Walshaw Archive / https://www.cise.ufl.edu/research/sparse/matrices/HB/bcsstk32.html
    private static final String STIFFNESS_MATRIX_FILENAME = "/Users/robertlindquist/Documents/thesis/data/walshaw_graphs/bcsstk31-parsed.txt";

    // from the Walshaw Archive / https://www.cise.ufl.edu/research/sparse/matrices/Cote/vibrobox.html
    private static final String VIBROBOX_FILENAME = "/Users/robertlindquist/Documents/thesis/data/walshaw_graphs/vibrobox-parsed.txt";

    private static final String CONFIG_FILE = "config.properties";

    private static final String OUTPUT_DIR = "/Users/robertlindquist/Documents/thesis/traces/";

    private static final int USERS_PER_PARTITION = 125;

    private static final int HOUR = 60;
    private static final int DAY = 24 * 60;
    private static final int NUM_ACTIONS = (7 * DAY) + 1; //one per minute for a week
    private static final int NUM_PARTITION_REMOVALS = 2;
    private static final Integer[] DOWNTIME_INDICES = {3*HOUR, 3*HOUR + DAY, 3*HOUR + 2*DAY, 3*HOUR + 3*DAY, 3*HOUR + 4*DAY, 3*HOUR + 5*DAY, 3*HOUR + 6*DAY};
    private static final Integer[] MIN_NUM_REPLICAS_OPTIONS = {0, 1, 2};
    private static final double PROB_RANDOM_FRIENDSHIP = 0.2;

    private static int maxUid = 0;
    private static int maxPid = 0;

    private static final Map<TRACE_ACTION, Double> actionsProbability = new HashMap<>();
    static {
        actionsProbability.put(BEFRIEND, 0.732D);     //Befriend/Unfriend ratio should be ~3, according to sigmod-linkbench paper
        actionsProbability.put(UNFRIEND, 0.244D);
        actionsProbability.put(ADD_USER, 0.018D);
        actionsProbability.put(REMOVE_USER, 0.006D);
        actionsProbability.put(ADD_PARTITION, 0D);    //this should be triggered by adding users or removing partitions...
        actionsProbability.put(REMOVE_PARTITION, 0D); //this is specified manually
        actionsProbability.put(DOWNTIME, 0D);         //this is also specified manually
    }

    private static final String INPUT_FILE = LESKOVEC_FACEBOOK_FILENAME;
    private static final String OUTPUT_FILENAME_STUB = OUTPUT_DIR + "facebook_";

    private static Map<Integer, Set<Integer>> bidirectionalFriendships;

    public static void main(String[] args) throws Exception {
        long nanoTime = System.nanoTime();
        Properties props = new Properties();
        props.load(new FileInputStream(CONFIG_FILE));
        String gpmetisLocation = props.getProperty("gpmetis.location");
        String gpmetisTempdir = props.getProperty("gpmetis.tempdir");
        BaseTraces baseTraces = generateTrace(gpmetisLocation, gpmetisTempdir);

        for(int minNumReplicas : baseTraces.getMinNumReplicasToHardStartTracesMap().keySet()) {
            String outputFile = OUTPUT_FILENAME_STUB + minNumReplicas + "_hard_" + nanoTime + ".txt";
            TraceTestUtils.writeTraceToFile(outputFile, baseTraces.getMinNumReplicasToHardStartTracesMap().get(minNumReplicas));
        }

        for(int minNumReplicas : baseTraces.getMinNumReplicasToSoftStartTracesMap().keySet()) {
            String outputFile = OUTPUT_FILENAME_STUB + minNumReplicas + "_soft_" + nanoTime + ".txt";
            TraceTestUtils.writeTraceToFile(outputFile, baseTraces.getMinNumReplicasToSoftStartTracesMap().get(minNumReplicas));
        }
    }

    private static BaseTraces generateTrace(String metisCommand, String metisTempDir) throws Exception {
        Map<Integer, Set<Integer>> originalFriendships = copyMapSet(TestUtils.extractFriendshipsFromFile(INPUT_FILE));
        bidirectionalFriendships = generateBidirectionalFriendshipSet(TestUtils.extractFriendshipsFromFile(INPUT_FILE));

        int numPids = 1 + (bidirectionalFriendships.size() / USERS_PER_PARTITION);

        int maxMinNumReplicas = new TreeSet<>(Arrays.asList(MIN_NUM_REPLICAS_OPTIONS)).last();
        if(numPids < 3 + maxMinNumReplicas) {
            numPids = 3 + maxMinNumReplicas;
        }
        Set<Integer> originalPids = new HashSet<>();
        for (int pid = 0; pid < numPids; pid++) {
            originalPids.add(pid);
        }

        Map<Integer, Set<Integer>> hardStartPartitions = TestUtils.getRandomPartitioning(originalPids, bidirectionalFriendships.keySet());
        Map<Integer, Set<Integer>> softStartPartitions = getSoftStartPartitions(originalPids, metisCommand, metisTempDir);
        Map<Integer, Map<Integer, Set<Integer>>> hardStartReplicasMap = new HashMap<>();
        Map<Integer, Map<Integer, Set<Integer>>> softStartReplicasMap = new HashMap<>();
        for(int minNumReplicas : MIN_NUM_REPLICAS_OPTIONS) {
            hardStartReplicasMap.put(minNumReplicas, TestUtils.getInitialReplicasObeyingKReplication(minNumReplicas, hardStartPartitions, bidirectionalFriendships));
            softStartReplicasMap.put(minNumReplicas, TestUtils.getInitialReplicasObeyingKReplication(minNumReplicas, softStartPartitions, bidirectionalFriendships));
        }

        List<FullTraceAction> actions = generateActions(new HashSet<>(originalPids));
        Map<Integer, BaseTrace> minNumReplicasToHardStartTracesMap = new HashMap<>();
        for(int minNumReplicas : hardStartReplicasMap.keySet()) {
            BaseTrace trace = new TraceWithReplicas(originalFriendships, hardStartPartitions, hardStartReplicasMap.get(minNumReplicas), actions);
            minNumReplicasToHardStartTracesMap.put(minNumReplicas, trace);
        }

        Map<Integer, BaseTrace> minNumReplicasToSoftStartTracesMap = new HashMap<>();
        for(int minNumReplicas : softStartReplicasMap.keySet()) {
            BaseTrace trace = new TraceWithReplicas(originalFriendships, softStartPartitions, softStartReplicasMap.get(minNumReplicas), actions);
            minNumReplicasToSoftStartTracesMap.put(minNumReplicas, trace);
        }

        return new BaseTraces(minNumReplicasToHardStartTracesMap, minNumReplicasToSoftStartTracesMap);
    }

    private static List<FullTraceAction> generateActions(Set<Integer> pids) {
        Set<Integer> possiblePartitionRemovalIndices = new HashSet<>(NUM_ACTIONS+1);
        for(int i=0; i<NUM_ACTIONS; i++) {
            possiblePartitionRemovalIndices.add(i);
        }
        possiblePartitionRemovalIndices.removeAll(Arrays.asList(DOWNTIME_INDICES));
        Set<Integer> partitionRemovalIndices = ProbabilityUtils.getKDistinctValuesFromList(NUM_PARTITION_REMOVALS, possiblePartitionRemovalIndices);

        int numPartitions = pids.size();
        int numUsers = bidirectionalFriendships.size();
        TRACE_ACTION[] script = new TRACE_ACTION[NUM_ACTIONS];
        for (int i = 0; i < NUM_ACTIONS; i++) {

            double usersPerPartition = ((double) numUsers) / numPartitions;
            if(Arrays.binarySearch(DOWNTIME_INDICES, i) >= 0) {
                script[i] = DOWNTIME;
            }
            else if(partitionRemovalIndices.contains(i)) {
                script[i] = REMOVE_PARTITION;
                numPartitions--;
            }
            else if(usersPerPartition > USERS_PER_PARTITION) {
                script[i] = ADD_PARTITION;
                numPartitions++;
            }
            else {
                TRACE_ACTION action = getActions(actionsProbability);
                if(action == ADD_USER) {
                    numUsers++;
                }
                else if(action == REMOVE_USER) {
                    numUsers--;
                }
                script[i] = action;
            }
        }

        maxPid = pids.size();
        maxUid = bidirectionalFriendships.size();

        List<FullTraceAction> actions = new LinkedList<>();
        for(int i=0; i<NUM_ACTIONS; i++) {
            switch (script[i]) {
                case ADD_USER:         actions.add(addU());     break;
                case REMOVE_USER:      actions.add(cutU());     break;
                case BEFRIEND:         actions.add(addF());     break;
                case UNFRIEND:         actions.add(cutF());     break;
                case ADD_PARTITION:    actions.add(addP(pids)); break;
                case REMOVE_PARTITION: actions.add(cutP(pids)); break;
                case DOWNTIME:         actions.add(new FullTraceAction(DOWNTIME));  break;
            }
        }

        return actions;
    }

    static FullTraceAction addU() {
        int newUid = ++maxUid;
        bidirectionalFriendships.put(newUid, new HashSet<Integer>());
        return new FullTraceAction(ADD_USER, newUid);
    }

    static FullTraceAction cutU() {
        int userToRemove = getRandomElement(bidirectionalFriendships.keySet());
        Set<Integer> friends = bidirectionalFriendships.get(userToRemove);
        if(friends.contains(userToRemove)) {
            throw new RuntimeException("User " + userToRemove + " is friends with itself");
        }

        for(int friendId : bidirectionalFriendships.get(userToRemove)) {
            bidirectionalFriendships.get(friendId).remove(userToRemove);
        }
        bidirectionalFriendships.remove(userToRemove);

        return new FullTraceAction(REMOVE_USER, userToRemove);
    }

    static FullTraceAction addF() {
        int uid = chooseKeyFromMapSetInProportionToSetSize(bidirectionalFriendships);
        List<Integer> friendIds = new LinkedList<>(bidirectionalFriendships.get(uid));

        //Grab a new friend either uniformly from friends of friends, or at random from everyone this user hasn't befriended
        if(Math.random() > PROB_RANDOM_FRIENDSHIP) {

            //We want all friends of this friend, except people who are already friends with uid
            List<Integer> friendsOfFriends = new LinkedList<>();
            for(int friendId : friendIds) {
                friendsOfFriends.addAll(bidirectionalFriendships.get(friendId));
            }
            friendsOfFriends.removeAll(Collections.singleton(uid));
            friendsOfFriends.removeAll(friendIds);
            if(friendsOfFriends.contains(uid)) {
                throw new RuntimeException("user " + uid + " might befriend itself");
            }
            if(!friendsOfFriends.isEmpty()) {
                int friendId = ProbabilityUtils.getRandomElement(friendsOfFriends);
                return innerBefriend(uid, friendId);
            }
        }

        //TODO: figure out how to select a nonFriendId with a preference towards those with a similar number of friends
        Set<Integer> nonFriendIds = new HashSet<>(bidirectionalFriendships.keySet());
        nonFriendIds.removeAll(friendIds);
        nonFriendIds.removeAll(Collections.singleton(uid));

        if(nonFriendIds.isEmpty()) {
            throw new RuntimeException("User " + uid + " is friends with everyone!");
        }

        if(nonFriendIds.contains(uid)) {
            throw new RuntimeException("user " + uid + " might befriend itself");
        }

        int friendId = chooseKeyFromMapSetWithCorrelationToSizeOfNode(bidirectionalFriendships, nonFriendIds, bidirectionalFriendships.get(uid).size());

        return innerBefriend(uid, friendId);
    }

    static int chooseKeyFromMapSetWithCorrelationToSizeOfNode(Map<Integer, Set<Integer>> mapset, Set<Integer> candidates, int numFriendsOfUser) {
        double numFriendsOfUserDouble = (double) numFriendsOfUser;
        List<Integer> keys = new ArrayList<>(mapset.size() * 100);
        for(int key : candidates) {
            int numEntries = getNumEntries((double) mapset.get(key).size(), numFriendsOfUserDouble);
            keys.addAll(nCopies(numEntries, key)); //this is intentional: we want numEntries copies of key in there
        }

        return ProbabilityUtils.getRandomElement(keys);
    }

    static int getNumEntries(double thisSize, double comparisonSize) {
        double lesser  = Math.min(thisSize, comparisonSize);
        double greater = Math.max(thisSize, comparisonSize);
        double similarity = Math.pow(lesser/greater, 2);
        return 1 + (int) (9 * similarity);
    }

    private static FullTraceAction innerBefriend(int oneUid, int theOtherUid) {
        int val1 = Math.min(oneUid, theOtherUid);
        int val2 = Math.max(oneUid, theOtherUid);
        bidirectionalFriendships.get(val1).add(val2);
        bidirectionalFriendships.get(val2).add(val1);
        return new FullTraceAction(BEFRIEND, val1, val2);
    }

    static FullTraceAction cutF() {
        List<Integer> friendship = chooseKeyValuePairFromMapSetUniformly(bidirectionalFriendships);
        int val1 = Math.min(friendship.get(0), friendship.get(1));
        int val2 = Math.max(friendship.get(0), friendship.get(1));
        bidirectionalFriendships.get(val1).remove(val2);
        bidirectionalFriendships.get(val2).remove(val1);

        return new FullTraceAction(UNFRIEND, val1, val2);
    }

    static FullTraceAction addP(Set<Integer> pids) {
        int newPid = ++maxPid;
        pids.add(newPid);
        return new FullTraceAction(ADD_PARTITION, newPid);
    }

    static FullTraceAction cutP(Set<Integer> pids) {
        int partitionToRemove = getRandomElement(pids);
        pids.remove(partitionToRemove);

        return new FullTraceAction(REMOVE_PARTITION, partitionToRemove);
    }

    private static class BaseTraces {
        private final Map<Integer, BaseTrace> minNumReplicasToHardStartTracesMap;
        private final Map<Integer, BaseTrace> minNumReplicasToSoftStartTracesMap;

        public BaseTraces(Map<Integer, BaseTrace> minNumReplicasToHardStartTracesMap, Map<Integer, BaseTrace> minNumReplicasToSoftStartTracesMap) {
            this.minNumReplicasToHardStartTracesMap = minNumReplicasToHardStartTracesMap;
            this.minNumReplicasToSoftStartTracesMap = minNumReplicasToSoftStartTracesMap;
        }

        public Map<Integer, BaseTrace> getMinNumReplicasToHardStartTracesMap() {
            return minNumReplicasToHardStartTracesMap;
        }

        public Map<Integer, BaseTrace> getMinNumReplicasToSoftStartTracesMap() {
            return minNumReplicasToSoftStartTracesMap;
        }
    }

    private static Map<Integer, Set<Integer>> getSoftStartPartitions(Set<Integer> pids, String metisCommand, String metisTempDir) {
        Map<Integer, Integer> softStartUidToPidMap = MetisRepartitioner.partition(metisCommand, metisTempDir, bidirectionalFriendships, pids);
        Map<Integer, Set<Integer>> softStartPartitions = new HashMap<>();
        for(int pid : pids) {
            softStartPartitions.put(pid, new HashSet<Integer>());
        }
        for(int uid : softStartUidToPidMap.keySet()) {
            int pid = softStartUidToPidMap.get(uid);
            softStartPartitions.get(pid).add(uid);
        }
        return softStartPartitions;
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
