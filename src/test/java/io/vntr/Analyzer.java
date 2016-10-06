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
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.getTopographyForMultigroupSocialNetwork;
import static io.vntr.TestUtils.initSet;

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

            Set<Integer> pids = new HashSet<Integer>();
            for(int pid = 0; pid < friendships.size() / usersPerPartition; pid++) {
                pids.add(pid);
            }

            Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
            Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(MIN_NUM_REPLICAS, partitions, friendships);

            double assortivity = ProbabilityUtils.calculateAssortivity(friendships);
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
            logger.warn("friendships:       " + friendships);
            logger.warn("partitions:        " + partitions);
            logger.warn("replicas:          " + replicas);

            JabejaManager  jabejaManager  = initJabejaManager (friendships, partitions);
            HermesManager  hermesManager  = initHermesManager (friendships, partitions);
            SparManager    sparManager    = initSparManager   (friendships, partitions, replicas);
            SpajaManager   spajaManager   = initSpajaManager  (friendships, partitions, replicas);
            SparmesManager sparmesManager = initSparmesManager(friendships, partitions, replicas);

            JabejaMiddleware  jabejaMiddleware  = initJabejaMiddleware (jabejaManager);
            HermesMiddleware  hermesMiddleware  = initHermesMiddleware (hermesManager);
            SparMiddleware    sparMiddleware    = initSparMiddleware   (sparManager);
            SpajaMiddleware   spajaMiddleware   = initSpajaMiddleware  (spajaManager);
            SparmesMiddleware sparmesMiddleware = initSparmesMiddleware(sparmesManager);

            ForestFireGenerator generator = new ForestFireGenerator(.34f, .34f, new TreeMap<Integer, Set<Integer>>(friendships));
            Map<Integer, Set<Integer>> newFriendships = generator.run();
//            runTest(jabejaMiddleware,  generator.getV(), newFriendships);
//            runTest(hermesMiddleware,  generator.getV(), newFriendships);
//            runTest(sparMiddleware,    generator.getV(), newFriendships);
//            runTest(spajaMiddleware,   generator.getV(), newFriendships);
            runTest(sparmesMiddleware, generator.getV(), newFriendships);
        }
    }

    private static <T extends IMiddleware & IMiddlewareAnalyzer> void runTest(T t, int newUid, Map<Integer, Set<Integer>> newFriendships) {
        long start = System.nanoTime();
        logger.warn("Beginning");
        logger.warn("\tEdge cut: " + t.getEdgeCut());
        logger.warn("\tReplication: " + t.getReplicationCount());


        Map<Integer, Set<Integer>> originalPartitions = t.getPartitionToUserMap();

        t.addUser(new User(newUid));
        int numNewFriendships = 0;
        for (Integer uid1 : newFriendships.keySet()) {
            for (Integer uid2 : newFriendships.get(uid1)) {
                t.befriend(uid1, uid2);
                numNewFriendships++;
            }
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
        List<Map<Integer, Set<Integer>>> friendshipsList = new ArrayList<Map<Integer, Set<Integer>>>(10);
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

            Set<Integer> pids = new HashSet<Integer>();
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

            ForestFireGenerator generator = new ForestFireGenerator(.34f, .34f, new TreeMap<Integer, Set<Integer>>(friendships));
            System.out.println("Starting generator");
            Map<Integer, Set<Integer>> newFriendships = generator.run();
            Integer newUid = generator.getV();
            System.out.println("Starting edge cuts");
            List<IMiddlewareAnalyzer> analyzers = Arrays.<IMiddlewareAnalyzer>asList(jabejaMiddleware, hermesMiddleware, sparMiddleware, spajaMiddleware, sparmesMiddleware);
            for(IMiddlewareAnalyzer iMiddlewareAnalyzer : analyzers) {
                long timeDiff = System.nanoTime() - start;
                System.out.println((timeDiff / 1000000) + "." + (timeDiff % 1000000) + "ms\n\t" + iMiddlewareAnalyzer + "\n\t\t" + iMiddlewareAnalyzer.getEdgeCut() + "\n\t\t" + iMiddlewareAnalyzer.getReplicationCount() + "\n");
            }
            List<IMiddleware> middlewares = Arrays.<IMiddleware>asList(jabejaMiddleware, hermesMiddleware, sparMiddleware, spajaMiddleware);//TODO: reenable this , sparmesMiddleware);
//            List<IMiddleware> middlewares = Arrays.<IMiddleware>asList(sparmesMiddleware);
            for(IMiddleware iMiddleware : middlewares) {
                System.out.println(iMiddleware + "\n");
                iMiddleware.addUser(new User(newUid));
                for(Integer uid1 : newFriendships.keySet()) {
                    for(Integer uid2 : newFriendships.get(uid1)) {
                        iMiddleware.befriend(uid1, uid2);
                    }
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
        return JabejaTestUtils.initGraph(1.5f, 2f, 0.2f, 9, partitions, friendships);
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
        return SparmesTestUtils.initGraph(2, 1.2f, true, partitions, friendships, replicas);
    }

    private SparmesMiddleware initSparmesMiddleware(SparmesManager manager) {
        return new SparmesMiddleware(manager);
    }

}
