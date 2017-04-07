package io.vntr;

import io.vntr.hermes.HermesManager;
import io.vntr.hermes.HermesMiddleware;
import io.vntr.hermes.HermesTestUtils;
import io.vntr.jabeja.JabejaManager;
import io.vntr.jabeja.JabejaMiddleware;
import io.vntr.jabeja.JabejaTestUtils;
import io.vntr.spaja.SpajaManager;
import io.vntr.spaja.SpajaMiddleware;
import io.vntr.spaja.SpajaTestUtils;
import io.vntr.spar.SparManager;
import io.vntr.spar.SparMiddleware;
import io.vntr.spar.SparTestUtils;
import io.vntr.sparmes.SparmesManager;
import io.vntr.sparmes.SparmesMiddleware;
import io.vntr.sparmes.SparmesTestUtils;
import io.vntr.utils.ProbabilityUtils;
import org.apache.log4j.Logger;

import java.util.*;

import static io.vntr.Analyzer.ACTIONS.*;
import static io.vntr.TestUtils.*;

/**
 * Created by robertlindquist on 10/4/16.
 */
public class Analyzer {
    final static Logger logger = Logger.getLogger(Analyzer.class);
    private static final String MISLOVE_FACEBOOK = "/Users/robertlindquist/Documents/thesis/data/mislove_wson_2009_facebook/mislove-facebook.txt";
    private static final String LESKOVEC_FACEBOOK = "/Users/robertlindquist/Documents/thesis/data/leskovec_facebook/leskovec-facebook.txt";
    private static final String ASU_FRIENDSTER = "/Users/robertlindquist/Documents/thesis/data/asu_friendster/asu-friendster.txt";

    private static final List<String> filenames = Arrays.asList(LESKOVEC_FACEBOOK, MISLOVE_FACEBOOK, ASU_FRIENDSTER);

    private static final int A_MILLION = 1000000;
    private static final int A_BILLION = 1000000000;
    private static final int USERS_PER_PARTITION = 100;
    private static final int MIN_NUM_REPLICAS = 2;

//    @Test
    public void testSomeStuff() throws Exception {
        for(int i=0; i<1000; i++) {
            int numUsers = 500 + (int) (Math.random() * 2000);
            int numGroups = 6 + (int) (Math.random() * 20);
            float groupProb = 0.03f + (float)(Math.random() * 0.1);
            float friendProb = 0.03f + (float)(Math.random() * 0.1);
            Map<Integer, Set<Integer>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, numGroups, groupProb, friendProb);

            int usersPerPartition = 50 + (int) (Math.random() * 100);

            Set<Integer> pids = new HashSet<>();
            for(int pid = 0; pid < friendships.size() / usersPerPartition; pid++) {
                pids.add(pid);
            }

            Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
            Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(MIN_NUM_REPLICAS, partitions, friendships);

            double assortivity = ProbabilityUtils.calculateAssortivityCoefficient(friendships);
            int numFriendships = 0;
            for(Integer uid : friendships.keySet()) {
                numFriendships += friendships.get(uid).size();
            }

            logger.warn("numUsers:          " + numUsers);
            logger.warn("numGroups:         " + numGroups);
            logger.warn("groupProb:         " + groupProb);
            logger.warn("friendProb:        " + friendProb);
            logger.warn("usersPerPartition: " + usersPerPartition);
            logger.warn("numFriendships:    " + numFriendships);
            logger.warn("assortivity:       " + assortivity);
//            logger.warn("friendships:       " + friendships);
//            logger.warn("partitions:        " + partitions);
//            logger.warn("replicas:          " + replicas);

            JabejaManager  jabejaManager  = initJabejaManager (friendships, partitions);
            HermesManager  hermesManager  = initHermesManager (friendships, partitions);
            SparManager    sparManager    = initSparManager   (friendships, partitions, replicas);
            SpajaManager   spajaManager   = initSpajaManager  (friendships, partitions, replicas);
//            SparmesManager sparmesManager = initSparmesManager(friendships, partitions, replicas);

            JabejaMiddleware  jabejaMiddleware  = initJabejaMiddleware (jabejaManager);
            HermesMiddleware  hermesMiddleware  = initHermesMiddleware (hermesManager);
            SparMiddleware    sparMiddleware    = initSparMiddleware   (sparManager);
            SpajaMiddleware   spajaMiddleware   = initSpajaMiddleware  (spajaManager);
//            SparmesMiddleware sparmesMiddleware = initSparmesMiddleware(sparmesManager);

            ForestFireGenerator generator = new ForestFireGenerator(.34f, .34f, new TreeMap<>(friendships));
            Set<Integer> newFriendships = generator.run();
            runTest(jabejaMiddleware,  generator.getV(), newFriendships);
            runTest(hermesMiddleware,  generator.getV(), newFriendships);
            runTest(sparMiddleware,    generator.getV(), newFriendships);
            runTest(spajaMiddleware,   generator.getV(), newFriendships);
//            runTest(sparmesMiddleware, generator.getV(), newFriendships);
        }
    }

    private static <T extends IMiddleware & IMiddlewareAnalyzer> void runTest(T t, int newUid, Set<Integer> newFriendships) {
        long start = System.nanoTime();
        logger.warn("Beginning " + t);
        logger.warn("\tEdge cut: " + t.getEdgeCut());
        logger.warn("\tReplication: " + t.getReplicationCount());

        Map<Integer, Set<Integer>> originalPartitions = t.getPartitionToUserMap();

        t.addUser(new User(newUid));
        int numNewFriendships = 0;
        for (Integer friendId : newFriendships) {
            t.befriend(friendId, newUid);
            numNewFriendships++;
        }

        Map<Integer, Set<Integer>> updatedPartitions = t.getPartitionToUserMap();

        long middle = System.nanoTime() - start;
        long middleSeconds = middle / A_BILLION;
        long middleMilliseconds = (middle % A_BILLION) / A_MILLION;
        logger.warn("After adding a user and " + numNewFriendships + " new friendships (T+" + middleSeconds + "." + middleMilliseconds + "s)");
        logger.warn("\tEdge cut: " + t.getEdgeCut());
        logger.warn("\tReplication: " + t.getReplicationCount());

        t.broadcastDowntime();

        long end = System.nanoTime() - start;
        long endSeconds = end / A_BILLION;
        long endMilliseconds = (end % A_BILLION) / A_MILLION;
        logger.warn("After broadcasting (T+" + endSeconds + "." + endMilliseconds + "s)");
        logger.warn("\tEdge cut: " + t.getEdgeCut());
        logger.warn("\tReplication: " + t.getReplicationCount());
    }

    //TODO: commit this reenabled once it all works
//    @Test
    public void testParsing() throws Exception {
        List<Map<Integer, Set<Integer>>> friendshipsList = new ArrayList<>(10);
        friendshipsList.add(getTopographyForMultigroupSocialNetwork(1000, 12, 0.1f, 0.1f));
        friendshipsList.add(getTopographyForMultigroupSocialNetwork(1300, 14, 0.09f, 0.09f));
        friendshipsList.add(getTopographyForMultigroupSocialNetwork(1600, 16, 0.09f, 0.09f));
        friendshipsList.add(getTopographyForMultigroupSocialNetwork(1900, 17, 0.08f, 0.08f));
        friendshipsList.add(getTopographyForMultigroupSocialNetwork(2200, 18, 0.08f, 0.08f));
        friendshipsList.add(getTopographyForMultigroupSocialNetwork(2500, 20, 0.07f, 0.07f));
        friendshipsList.add(TestUtils.extractFriendshipsFromFile(LESKOVEC_FACEBOOK));
//        friendshipsList.add(TestUtils.extractFriendshipsFromFile(MISLOVE_FACEBOOK));
//        friendshipsList.add(TestUtils.extractFriendshipsFromFile(ASU_FRIENDSTER));

        long start = System.nanoTime();
        for(Map<Integer, Set<Integer>> friendships : friendshipsList) {
            System.out.println("\nSize: " + friendships.size());

            Set<Integer> pids = new HashSet<>();
            for(int pid = 0; pid < friendships.size() / USERS_PER_PARTITION; pid++) {
                pids.add(pid);
            }
            Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
            Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(MIN_NUM_REPLICAS, partitions, friendships);


            System.out.println("Starting Ja-be-Ja");
            JabejaManager jabejaManager = initJabejaManager(friendships, partitions);
            System.out.println("Starting Hermes");
            HermesManager hermesManager = initHermesManager(friendships, partitions);
            System.out.println("Starting SPAR");
            SparManager sparManager = initSparManager(friendships, partitions, replicas);
            System.out.println("Starting Spaja");
            SpajaManager spajaManager = initSpajaManager(friendships, partitions, replicas);
            System.out.println("Starting Sparmes");
            SparmesManager sparmesManager = initSparmesManager(friendships, partitions, replicas);

            JabejaMiddleware  jabejaMiddleware  = initJabejaMiddleware(jabejaManager);
            HermesMiddleware  hermesMiddleware  = initHermesMiddleware(hermesManager);
            SparMiddleware    sparMiddleware    = initSparMiddleware(sparManager);
            SpajaMiddleware   spajaMiddleware   = initSpajaMiddleware(spajaManager);
            SparmesMiddleware sparmesMiddleware = initSparmesMiddleware(sparmesManager);

            ForestFireGenerator generator = new ForestFireGenerator(.34f, .34f, new TreeMap<>(friendships));
            System.out.println("Starting generator");
            Set<Integer> newFriendships = generator.run();
            Integer newUid = generator.getV();
            System.out.println("Starting edge cuts");
            List<IMiddlewareAnalyzer> analyzers = Arrays.asList(jabejaMiddleware, hermesMiddleware, sparMiddleware, spajaMiddleware, sparmesMiddleware);
            for(IMiddlewareAnalyzer iMiddlewareAnalyzer : analyzers) {
                long timeDiff = System.nanoTime() - start;
                System.out.println((timeDiff / 1000000) + "." + (timeDiff % 1000000) + "ms\n\t" + iMiddlewareAnalyzer + "\n\t\t" + iMiddlewareAnalyzer.getEdgeCut() + "\n\t\t" + iMiddlewareAnalyzer.getReplicationCount() + "\n");
            }
            List<IMiddleware> middlewares = Arrays.<IMiddleware>asList(jabejaMiddleware, hermesMiddleware, sparMiddleware, spajaMiddleware);//TODO: reenable this , sparmesMiddleware);
//            List<IMiddleware> middlewares = Arrays.<IMiddleware>asList(sparmesMiddleware);
            for(IMiddleware iMiddleware : middlewares) {
                System.out.println(iMiddleware + "\n");
                iMiddleware.addUser(new User(newUid));
                for(Integer friendId : newFriendships) {
                    iMiddleware.befriend(friendId, newUid);
                }
                iMiddleware.broadcastDowntime();
            }
            for(IMiddlewareAnalyzer iMiddlewareAnalyzer : analyzers) {
                long timeDiff = System.nanoTime() - start;
                System.out.println((timeDiff / 1000000) + "." + (timeDiff % 1000000) + "ms\n\t" + iMiddlewareAnalyzer + "\n\t\t" + iMiddlewareAnalyzer.getEdgeCut() + "\n\t\t" + iMiddlewareAnalyzer.getReplicationCount() + "\n");
            }
        }
    }

    private SparManager initSparManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SparTestUtils.initGraph(2, partitions, friendships, replicas);
    }

    private SparMiddleware initSparMiddleware(SparManager manager) {
        return new SparMiddleware(manager);
    }

    private HermesManager initHermesManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) throws Exception {
        return HermesTestUtils.initGraph(1.2f, true, partitions, friendships);
    }

    private HermesMiddleware initHermesMiddleware(HermesManager manager) {
        return new HermesMiddleware(manager, 1.2f);
    }

    private JabejaManager initJabejaManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) throws Exception {
        return JabejaTestUtils.initGraph(1.5f, 2f, 0.2f, 2f, 0.2f, 9, partitions, friendships);
    }

    private JabejaMiddleware initJabejaMiddleware(JabejaManager manager) {
        return new JabejaMiddleware(manager);
    }

    private SpajaManager initSpajaManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SpajaTestUtils.initGraph(2, 1.5f, 2f, 0.2f, 9, partitions, friendships, replicas);
    }

    private SpajaMiddleware initSpajaMiddleware(SpajaManager manager) {
        return new SpajaMiddleware(manager);
    }

    private SparmesManager initSparmesManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SparmesTestUtils.initGraph(2, 1.2f, 3, true, partitions, friendships, replicas);
    }

    private SparmesMiddleware initSparmesMiddleware(SparmesManager manager) {
        return new SparmesMiddleware(manager);
    }

    public enum ACTIONS { ADD_USER, REMOVE_USER, BEFRIEND, UNFRIEND, ADD_PARTITION, REMOVE_PARTITION, DOWNTIME, FOREST_FIRE }

    //@Test
    public void stressTest() throws Exception {
        for(int i=0; i<1000; i++) {
            int numUsers = 500 + (int) (Math.random() * 2000);
            int numGroups = 6 + (int) (Math.random() * 20);
            float groupProb = 0.03f + (float) (Math.random() * 0.1);
            float friendProb = 0.03f + (float) (Math.random() * 0.1);
            Map<Integer, Set<Integer>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, numGroups, groupProb, friendProb);

            int usersPerPartition = 50 + (int) (Math.random() * 100);

            Set<Integer> pids = new HashSet<>();
            for (int pid = 0; pid < friendships.size() / usersPerPartition; pid++) {
                pids.add(pid);
            }

            Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
            Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(MIN_NUM_REPLICAS, partitions, friendships);

            double assortivity = ProbabilityUtils.calculateAssortivityCoefficient(friendships);
            int numFriendships = 0;
            for (Integer uid : friendships.keySet()) {
                numFriendships += friendships.get(uid).size();
            }

            logger.warn("\nnumUsers:          " + numUsers);
            logger.warn("numGroups:         " + numGroups);
            logger.warn("groupProb:         " + groupProb);
            logger.warn("friendProb:        " + friendProb);
            logger.warn("usersPerPartition: " + usersPerPartition);
            logger.warn("numFriendships:    " + numFriendships);
            logger.warn("assortivity:       " + assortivity);
            logger.warn("friendships:       " + friendships);
            logger.warn("partitions:        " + partitions);
            logger.warn("replicas:          " + replicas);

            JabejaManager jabejaManager = initJabejaManager(friendships, partitions);
            HermesManager hermesManager = initHermesManager(friendships, partitions);
            SparManager sparManager = initSparManager(friendships, partitions, replicas);
            SpajaManager spajaManager = initSpajaManager(friendships, partitions, replicas);
            SparmesManager sparmesManager = initSparmesManager(friendships, partitions, replicas);

            JabejaMiddleware jabejaMiddleware = initJabejaMiddleware(jabejaManager);
            HermesMiddleware hermesMiddleware = initHermesMiddleware(hermesManager);
            SparMiddleware sparMiddleware = initSparMiddleware(sparManager);
            SpajaMiddleware spajaMiddleware = initSpajaMiddleware(spajaManager);
            SparmesMiddleware sparmesMiddleware = initSparmesMiddleware(sparmesManager);

            Map<ACTIONS, Double> actionsProbability = new HashMap<>();
            actionsProbability.put(ADD_USER,         0.125D);
            actionsProbability.put(REMOVE_USER,      0.05D);
            actionsProbability.put(BEFRIEND,         0.64D);
            actionsProbability.put(UNFRIEND,         0.05D);
            actionsProbability.put(FOREST_FIRE,      0.05D);
            actionsProbability.put(ADD_PARTITION,    0.05D);
            actionsProbability.put(REMOVE_PARTITION, 0.01D);
            actionsProbability.put(DOWNTIME,         0.025D);

            ACTIONS[] script = new ACTIONS[10001];
            for(int j=0; j<script.length-1; j++) {
                script[j] = getActions(actionsProbability);
            }
            script[script.length-1] = DOWNTIME;

            runScriptedTest(sparMiddleware,    script);
            runScriptedTest(jabejaMiddleware,  script);
            runScriptedTest(hermesMiddleware,  script);
            runScriptedTest(spajaMiddleware,   script);
            runScriptedTest(sparmesMiddleware, script);
        }
    }

    <T extends IMiddleware & IMiddlewareAnalyzer> void runScriptedTest(T middleware, ACTIONS[] script) {
        for(int i=0; i<script.length; i++) {
            ACTIONS action = script[i];
            if(action == ADD_USER) {
                logger.warn("(" + i + ")" + ADD_USER + ": pre");
                int newUid = middleware.addUser();
                logger.warn("(" + i + ")" + ADD_USER + ": " + newUid);
            }
            if(action == REMOVE_USER) {
                int badId = ProbabilityUtils.getRandomElement(middleware.getUserIds());
                logger.warn("(" + i + ")" + REMOVE_USER + ": " + badId);
                middleware.removeUser(badId);
            }
            if(action == BEFRIEND) {
                Set<Integer> friends = ProbabilityUtils.getKDistinctValuesFromList(2, middleware.getUserIds());
                List<Integer> friendList = new LinkedList<>(friends);
                logger.warn("(" + i + ")" + BEFRIEND + ": " + friendList.get(0) + "<->" + friendList.get(1));
                middleware.befriend(friendList.get(0), friendList.get(1));
            }
            if(action == UNFRIEND) {
                Set<Integer> frenemies = ProbabilityUtils.getKDistinctValuesFromList(2, middleware.getUserIds());
                List<Integer> frenemyList = new LinkedList<>(frenemies);
                logger.warn("(" + i + ")" + UNFRIEND + ": " + frenemyList.get(0) + "<->" + frenemyList.get(1));
                middleware.unfriend(frenemyList.get(0), frenemyList.get(1));
            }
            if(action == FOREST_FIRE) {
                ForestFireGenerator generator = new ForestFireGenerator(.34f, .34f, new TreeMap<>(middleware.getFriendships()));
                Set<Integer> newUsersFriends = generator.run();
                int newUid = generator.getV();
                logger.warn("(" + i + ")" + FOREST_FIRE + ": " + newUid + "<->" + newUsersFriends);
                middleware.addUser(new User(newUid));
                for(Integer friend : newUsersFriends) {
                    middleware.befriend(newUid, friend);
                }
            }
            if(action == ADD_PARTITION) {
                logger.warn("(" + i + ")" + ADD_PARTITION + ": pre");
                int newPid = middleware.addPartition();
                logger.warn("(" + i + ")" + ADD_PARTITION + ": " + newPid);
            }
            if(action == REMOVE_PARTITION) {
                int badId = ProbabilityUtils.getRandomElement(middleware.getPartitionIds());
                logger.warn("(" + i + ")" + REMOVE_PARTITION + ": " + badId);
                middleware.removePartition(badId);
            }
        }
    }

    static ACTIONS getActions(Map<ACTIONS, Double> actionsProbability) {
        double random = Math.random();

        for(ACTIONS action : ACTIONS.values()) {
            if(random < actionsProbability.get(action)) {
                return action;
            }
            random -= actionsProbability.get(action);
        }

        return DOWNTIME;
    }
}
