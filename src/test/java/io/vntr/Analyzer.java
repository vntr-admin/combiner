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
import org.junit.Test;

import java.util.*;

import static io.vntr.TestUtils.initSet;

/**
 * Created by robertlindquist on 10/4/16.
 */
public class Analyzer {
    private static final String MISLOVE_FACEBOOK = "/Users/robertlindquist/Documents/thesis/data/mislove_wson_2009_facebook/mislove-facebook.txt";
    private static final String LESKOVEC_FACEBOOK = "/Users/robertlindquist/Documents/thesis/data/leskovec_facebook/leskovec-facebook.txt";
    private static final String ASU_FRIENDSTER = "/Users/robertlindquist/Documents/thesis/data/asu_friendster/asu-friendster.txt";

    private static final List<String> filenames = Arrays.asList(LESKOVEC_FACEBOOK, MISLOVE_FACEBOOK, ASU_FRIENDSTER);

    private static final int USERS_PER_PARTITION = 100;
    private static final int MIN_NUM_REPLICAS = 2;

    //TODO: commit this reenabled once it all works
//    @Test
    public void testParsing() throws Exception {
        for(String filename : filenames) {
            System.out.println(filename);
            Map<Long, Set<Long>> friendships = TestUtils.extractFriendshipsFromFile(filename);

            Set<Long> pids = new HashSet<Long>();
            for(long pid = 0; pid < friendships.size() / USERS_PER_PARTITION; pid++) {
                pids.add(pid);
            }
            Map<Long, Set<Long>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());
            Map<Long, Set<Long>> replicas = TestUtils.getInitialReplicasObeyingKReplication(MIN_NUM_REPLICAS, partitions, friendships);


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

            //TODO: figure out how to hook this up to the above and run it
            ForestFireGenerator generator = new ForestFireGenerator(.34, .34, new TreeMap<Long, Set<Long>>(friendships));
            System.out.println("Starting generator");
            Map<Long, Set<Long>> newFriendships = generator.run();
            Long newUid = generator.getV();
            System.out.println("Starting edge cuts");
            List<IMiddlewareAnalyzer> analyzers = Arrays.<IMiddlewareAnalyzer>asList(jabejaMiddleware, hermesMiddleware, sparMiddleware, spajaMiddleware, sparmesMiddleware);
            for(IMiddlewareAnalyzer iMiddlewareAnalyzer : analyzers) {
                System.out.println(iMiddlewareAnalyzer + "\n\t" + iMiddlewareAnalyzer.getEdgeCut() + "\n");
            }
            List<IMiddleware> middlewares = Arrays.<IMiddleware>asList(jabejaMiddleware, hermesMiddleware, sparMiddleware, spajaMiddleware, sparmesMiddleware);
//            List<IMiddleware> middlewares = Arrays.<IMiddleware>asList(sparmesMiddleware);
            for(IMiddleware iMiddleware : middlewares) {
                System.out.println(iMiddleware + "\n");
                iMiddleware.addUser(new User("User " + newUid, newUid));
                for(Long uid1 : newFriendships.keySet()) {
                    for(Long uid2 : newFriendships.get(uid1)) {
                        iMiddleware.befriend(uid1, uid2);
                    }
                }
                iMiddleware.broadcastDowntime();
            }
            for(IMiddlewareAnalyzer iMiddlewareAnalyzer : analyzers) {
                System.out.println(iMiddlewareAnalyzer + "\n\t" + iMiddlewareAnalyzer.getEdgeCut() + "\n");
            }
        }
    }

    private SparManager initSparManager(Map<Long, Set<Long>> friendships, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> replicas) {
        return SparTestUtils.initGraph(2, partitions, friendships, replicas);
    }

    private SparMiddleware initSparMiddleware(SparManager manager) {
        return new SparMiddleware(manager);
    }

    private HermesManager initHermesManager(Map<Long, Set<Long>> friendships, Map<Long, Set<Long>> partitions) throws Exception {
        return HermesTestUtils.initGraph(1.2, partitions, friendships);
    }

    private HermesMiddleware initHermesMiddleware(HermesManager manager) {
        return new HermesMiddleware(manager, 1.2);
    }

    private JabejaManager initJabejaManager(Map<Long, Set<Long>> friendships, Map<Long, Set<Long>> partitions) throws Exception {
        return JabejaTestUtils.initGraph(1.5, 2D, 0.2D, 9, partitions, friendships);
    }

    private JabejaMiddleware initJabejaMiddleware(JabejaManager manager) {
        return new JabejaMiddleware(manager);
    }

    private SpajaManager initSpajaManager(Map<Long, Set<Long>> friendships, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> replicas) {
        return SpajaTestUtils.initGraph(2, 1.5, 2D, 0.2D, 9, partitions, friendships, replicas);
    }

    private SpajaMiddleware initSpajaMiddleware(SpajaManager manager) {
        return new SpajaMiddleware(manager);
    }

    private SparmesManager initSparmesManager(Map<Long, Set<Long>> friendships, Map<Long, Set<Long>> partitions, Map<Long, Set<Long>> replicas) {
        return SparmesTestUtils.initGraph(2, 1.2, partitions, friendships, replicas);
    }

    private SparmesMiddleware initSparmesMiddleware(SparmesManager manager) {
        return new SparmesMiddleware(manager);
    }

}
