package io.vntr.trace;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.list.TShortList;
import gnu.trove.list.array.TShortArrayList;
import gnu.trove.list.linked.TShortLinkedList;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import io.vntr.TestUtils;
import io.vntr.repartition.MetisRepartitioner;
import io.vntr.utils.TroveUtils;

import java.io.FileInputStream;
import java.util.*;

import static io.vntr.trace.TRACE_ACTION.*;
import static io.vntr.utils.ProbabilityUtils.chooseKeyFromMapSetInProportionToSetSize;
import static io.vntr.utils.ProbabilityUtils.chooseKeyValuePairFromMapSetUniformly;
import static io.vntr.utils.TroveUtils.convert;
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

    private static final short USERS_PER_PARTITION = 125;

    private static final short HOUR = 60;
    private static final short DAY = 24 * 60;
    private static final short NUM_ACTIONS = (7 * DAY) + 1; //one per minute for a week
    private static final short NUM_PARTITION_REMOVALS = 2;
    private static final short[] DOWNTIME_INDICES = {3*HOUR, 3*HOUR + DAY, 3*HOUR + 2*DAY, 3*HOUR + 3*DAY, 3*HOUR + 4*DAY, 3*HOUR + 5*DAY, 3*HOUR + 6*DAY};
    private static final short[] MIN_NUM_REPLICAS_OPTIONS = {0, 1, 2};
    private static final double PROB_RANDOM_FRIENDSHIP = 0.2;

    private static short maxUid = 0;
    private static short maxPid = 0;

    private static final Map<TRACE_ACTION, Double> actionsProbability = new HashMap<>();
    static {
        actionsProbability.put(BEFRIEND, 0.738D);     //Befriend/Unfriend ratio should be ~3, according to sigmod-linkbench paper
        actionsProbability.put(UNFRIEND, 0.246D);
        actionsProbability.put(ADD_USER, 0.012D);
        actionsProbability.put(REMOVE_USER, 0.004D);
        actionsProbability.put(ADD_PARTITION, 0D);    //this should be triggered by adding users or removing partitions...
        actionsProbability.put(REMOVE_PARTITION, 0D); //this is specified manually
        actionsProbability.put(DOWNTIME, 0D);         //this is also specified manually
    }

    private static final String INPUT_FILE = LESKOVEC_FACEBOOK_FILENAME;
    private static final String OUTPUT_FILENAME_STUB = OUTPUT_DIR + "facebook_";

    private static TShortObjectMap<TShortSet> bidirectionalFriendships;

    public static void main(String[] args) throws Exception {
        long nanoTime = System.nanoTime();
        Properties props = new Properties();
        props.load(new FileInputStream(CONFIG_FILE));
        String gpmetisLocation = props.getProperty("gpmetis.location");
        String gpmetisTempdir = props.getProperty("gpmetis.tempdir");
        BaseTraces baseTraces = generateTrace(gpmetisLocation, gpmetisTempdir);

        for(short minNumReplicas : baseTraces.getMinNumReplicasToHardStartTracesMap().keys()) {
            String outputFile = OUTPUT_FILENAME_STUB + minNumReplicas + "_hard_" + nanoTime + ".txt";
            TraceTestUtils.writeTraceToFile(outputFile, baseTraces.getMinNumReplicasToHardStartTracesMap().get(minNumReplicas));
        }

        for(short minNumReplicas : baseTraces.getMinNumReplicasToSoftStartTracesMap().keys()) {
            String outputFile = OUTPUT_FILENAME_STUB + minNumReplicas + "_soft_" + nanoTime + ".txt";
            TraceTestUtils.writeTraceToFile(outputFile, baseTraces.getMinNumReplicasToSoftStartTracesMap().get(minNumReplicas));
        }
    }

    private static BaseTraces generateTrace(String metisCommand, String metisTempDir) throws Exception {
        TShortObjectMap<TShortSet> originalFriendships = TroveUtils.copyTShortObjectMapIntSet(TestUtils.extractFriendshipsFromFile(INPUT_FILE));
        bidirectionalFriendships = TroveUtils.generateBidirectionalFriendshipSet(TestUtils.extractFriendshipsFromFile(INPUT_FILE));

        short numPids = (short)(1 + (bidirectionalFriendships.size() / USERS_PER_PARTITION));

        short maxMinNumReplicas = TroveUtils.max(new TShortHashSet(MIN_NUM_REPLICAS_OPTIONS));
        if(numPids < 3 + maxMinNumReplicas) {
            numPids = (short)(3 + maxMinNumReplicas);
        }
        TShortSet originalPids = new TShortHashSet(numPids+1);
        for (short pid = 0; pid < numPids; pid++) {
            originalPids.add(pid);
        }

        TShortObjectMap<TShortSet> hardStartPartitions = TestUtils.getRandomPartitioning(originalPids, bidirectionalFriendships.keySet());
        TShortObjectMap<TShortSet> softStartPartitions = getSoftStartPartitions(originalPids, metisCommand, metisTempDir);
        TShortObjectMap<TShortObjectMap<TShortSet>> hardStartReplicasMap = new TShortObjectHashMap<>();
        TShortObjectMap<TShortObjectMap<TShortSet>> softStartReplicasMap = new TShortObjectHashMap<>();
        for(short minNumReplicas : MIN_NUM_REPLICAS_OPTIONS) {
            hardStartReplicasMap.put(minNumReplicas, TroveUtils.getInitialReplicasObeyingKReplication(minNumReplicas, hardStartPartitions, bidirectionalFriendships));
            softStartReplicasMap.put(minNumReplicas, TroveUtils.getInitialReplicasObeyingKReplication(minNumReplicas, softStartPartitions, bidirectionalFriendships));
        }

        List<FullTraceAction> actions = generateActions(new TShortHashSet(originalPids));
        TShortObjectMap<BaseTrace> minNumReplicasToHardStartTracesMap = new TShortObjectHashMap<>();
        for(short minNumReplicas : hardStartReplicasMap.keys()) {
            BaseTrace trace = new TraceWithReplicas(originalFriendships, hardStartPartitions, hardStartReplicasMap.get(minNumReplicas), actions);
            minNumReplicasToHardStartTracesMap.put(minNumReplicas, trace);
        }

        TShortObjectMap<BaseTrace> minNumReplicasToSoftStartTracesMap = new TShortObjectHashMap<>();
        for(short minNumReplicas : softStartReplicasMap.keys()) {
            BaseTrace trace = new TraceWithReplicas(originalFriendships, softStartPartitions, softStartReplicasMap.get(minNumReplicas), actions);
            minNumReplicasToSoftStartTracesMap.put(minNumReplicas, trace);
        }

        return new BaseTraces(minNumReplicasToHardStartTracesMap, minNumReplicasToSoftStartTracesMap);
    }

    private static List<FullTraceAction> generateActions(TShortSet pids) {
        TShortSet possiblePartitionRemovalIndices = new TShortHashSet(NUM_ACTIONS+1);
        for(short i=0; i<NUM_ACTIONS; i++) {
            possiblePartitionRemovalIndices.add(i);
        }
        possiblePartitionRemovalIndices.removeAll(Arrays.asList(DOWNTIME_INDICES));
        TShortSet partitionRemovalIndices = TroveUtils.getKDistinctValuesFromArray(NUM_PARTITION_REMOVALS, possiblePartitionRemovalIndices.toArray());

        short numPartitions = (short)pids.size();
        short numUsers = (short)bidirectionalFriendships.size();
        TRACE_ACTION[] script = new TRACE_ACTION[NUM_ACTIONS];
        for (short i = 0; i < NUM_ACTIONS; i++) {

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

        maxPid = (short)pids.size();
        maxUid = (short)bidirectionalFriendships.size();

        List<FullTraceAction> actions = new LinkedList<>();
        for(short i=0; i<NUM_ACTIONS; i++) {
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
        short newUid = ++maxUid;
        bidirectionalFriendships.put(newUid, new TShortHashSet());
        return new FullTraceAction(ADD_USER, newUid);
    }

    static FullTraceAction cutU() {
        short userToRemove = TroveUtils.getRandomElement(bidirectionalFriendships.keySet());
        TShortSet friends = bidirectionalFriendships.get(userToRemove);
        if(friends.contains(userToRemove)) {
            throw new RuntimeException("User " + userToRemove + " is friends with itself");
        }

        for(TShortIterator iter = bidirectionalFriendships.get(userToRemove).iterator(); iter.hasNext(); ) {
            bidirectionalFriendships.get(iter.next()).remove(userToRemove);
        }
        bidirectionalFriendships.remove(userToRemove);

        return new FullTraceAction(REMOVE_USER, userToRemove);
    }

    static FullTraceAction addF() {
        short uid = chooseKeyFromMapSetInProportionToSetSize(bidirectionalFriendships);
        TShortList friendIds = new TShortArrayList(bidirectionalFriendships.get(uid));

        //Grab a new friend either uniformly from friends of friends, or at random from everyone this user hasn't befriended
        if(Math.random() > PROB_RANDOM_FRIENDSHIP) {

            //We want all friends of this friend, except people who are already friends with uid
            TShortList friendsOfFriends = new TShortLinkedList();
            for(short i=0; i<friendIds.size(); i++) {
                friendsOfFriends.addAll(convert(bidirectionalFriendships.get(friendIds.get(i))));
            }
            friendsOfFriends.removeAll(Collections.singleton(uid));
            friendsOfFriends.removeAll(friendIds);
            if(friendsOfFriends.contains(uid)) {
                throw new RuntimeException("user " + uid + " might befriend itself");
            }
            if(!friendsOfFriends.isEmpty()) {
                short friendId = friendIds.get((short) (Math.random() * friendsOfFriends.size()));
                return innerBefriend(uid, friendId);
            }
        }

        //TODO: figure out how to select a nonFriendId with a preference towards those with a similar number of friends
        TShortSet nonFriendIds = new TShortHashSet(bidirectionalFriendships.keySet());
        nonFriendIds.removeAll(friendIds);
        nonFriendIds.removeAll(Collections.singleton(uid));

        if(nonFriendIds.isEmpty()) {
            throw new RuntimeException("User " + uid + " is friends with everyone!");
        }

        if(nonFriendIds.contains(uid)) {
            throw new RuntimeException("user " + uid + " might befriend itself");
        }

        short friendId = chooseKeyFromMapSetWithCorrelationToSizeOfNode(bidirectionalFriendships, nonFriendIds, (short)bidirectionalFriendships.get(uid).size());

        return innerBefriend(uid, friendId);
    }

    static short chooseKeyFromMapSetWithCorrelationToSizeOfNode(TShortObjectMap<TShortSet> mapset, TShortSet candidates, short numFriendsOfUser) {
        double numFriendsOfUserDouble = (double) numFriendsOfUser;
        TShortList keys = new TShortArrayList(mapset.size() * 100);
        for(TShortIterator iter = candidates.iterator(); iter.hasNext(); ) {
            short key = iter.next();
            short numEntries = getNumEntries((double) mapset.get(key).size(), numFriendsOfUserDouble);
            keys.addAll(nCopies(numEntries, key)); //this is intentional: we want numEntries copies of key in there
        }

        return keys.get((short) (Math.random() * keys.size()));
    }

    static short getNumEntries(double thisSize, double comparisonSize) {
        double lesser  = Math.min(thisSize, comparisonSize);
        double greater = Math.max(thisSize, comparisonSize);
        double similarity = Math.pow(lesser/greater, 2);
        return (short)(1 + (9 * similarity));
    }

    private static FullTraceAction innerBefriend(short oneUid, short theOtherUid) {
        short val1 = (short)Math.min(oneUid, theOtherUid);
        short val2 = (short)Math.max(oneUid, theOtherUid);
        bidirectionalFriendships.get(val1).add(val2);
        bidirectionalFriendships.get(val2).add(val1);
        return new FullTraceAction(BEFRIEND, val1, val2);
    }

    static FullTraceAction cutF() {
        List<Short> friendship = chooseKeyValuePairFromMapSetUniformly(bidirectionalFriendships);
        short val1 = (short)Math.min(friendship.get(0), friendship.get(1));
        short val2 = (short)Math.max(friendship.get(0), friendship.get(1));
        bidirectionalFriendships.get(val1).remove(val2);
        bidirectionalFriendships.get(val2).remove(val1);

        return new FullTraceAction(UNFRIEND, val1, val2);
    }

    static FullTraceAction addP(TShortSet pids) {
        short newPid = ++maxPid;
        pids.add(newPid);
        return new FullTraceAction(ADD_PARTITION, newPid);
    }

    static FullTraceAction cutP(TShortSet pids) {
        short partitionToRemove = TroveUtils.getRandomElement(pids);
        pids.remove(partitionToRemove);

        return new FullTraceAction(REMOVE_PARTITION, partitionToRemove);
    }

    private static class BaseTraces {
        private final TShortObjectMap<BaseTrace> minNumReplicasToHardStartTracesMap;
        private final TShortObjectMap<BaseTrace> minNumReplicasToSoftStartTracesMap;

        public BaseTraces(TShortObjectMap<BaseTrace> minNumReplicasToHardStartTracesMap, TShortObjectMap<BaseTrace> minNumReplicasToSoftStartTracesMap) {
            this.minNumReplicasToHardStartTracesMap = minNumReplicasToHardStartTracesMap;
            this.minNumReplicasToSoftStartTracesMap = minNumReplicasToSoftStartTracesMap;
        }

        public TShortObjectMap<BaseTrace> getMinNumReplicasToHardStartTracesMap() {
            return minNumReplicasToHardStartTracesMap;
        }

        public TShortObjectMap<BaseTrace> getMinNumReplicasToSoftStartTracesMap() {
            return minNumReplicasToSoftStartTracesMap;
        }
    }

    private static TShortObjectMap<TShortSet> getSoftStartPartitions(TShortSet pids, String metisCommand, String metisTempDir) {
        TShortShortMap softStartUidToPidMap = MetisRepartitioner.partition(metisCommand, metisTempDir, bidirectionalFriendships, new TShortHashSet(pids));
        TShortObjectMap<TShortSet> softStartPartitions = new TShortObjectHashMap<>(pids.size()+1);
        for(TShortIterator iter = pids.iterator(); iter.hasNext(); ) {
            softStartPartitions.put(iter.next(), new TShortHashSet());
        }
        for(short uid : softStartUidToPidMap.keys()) {
            short pid = softStartUidToPidMap.get(uid);
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
