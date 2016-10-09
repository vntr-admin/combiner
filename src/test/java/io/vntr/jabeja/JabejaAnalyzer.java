package io.vntr.jabeja;

import io.vntr.Analyzer;
import io.vntr.ForestFireGenerator;
import io.vntr.TestUtils;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.*;

import static io.vntr.Analyzer.ACTIONS.*;
import static io.vntr.TestUtils.*;
import static org.junit.Assert.*;

/**
 * Created by robertlindquist on 10/8/16.
 */
public class JabejaAnalyzer {

    final static Logger logger = Logger.getLogger(JabejaAnalyzer.class);

//    @Test
    public void stressTest() throws Exception {
        for(int i=0; i<1000; i++) {
            int numUsers = 500 + (int) (Math.random() * 2000);
            int numGroups = 6 + (int) (Math.random() * 20);
            float groupProb = 0.03f + (float) (Math.random() * 0.1);
            float friendProb = 0.03f + (float) (Math.random() * 0.1);
            Map<Integer, Set<Integer>> friendships = getTopographyForMultigroupSocialNetwork(numUsers, numGroups, groupProb, friendProb);

            int usersPerPartition = 50 + (int) (Math.random() * 100);

            Set<Integer> pids = new HashSet<Integer>();
            for (int pid = 0; pid < friendships.size() / usersPerPartition; pid++) {
                pids.add(pid);
            }

            Map<Integer, Set<Integer>> partitions = TestUtils.getRandomPartitioning(pids, friendships.keySet());

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

            JabejaManager sparManager = initJabejaManager(friendships, partitions);
            JabejaMiddleware sparMiddleware = initJabejaMiddleware(sparManager);

            Map<Analyzer.ACTIONS, Double> actionsProbability = new HashMap<Analyzer.ACTIONS, Double>();
            actionsProbability.put(ADD_USER,         0.125D);
            actionsProbability.put(REMOVE_USER,      0.05D);
            actionsProbability.put(BEFRIEND,         0.64D);
            actionsProbability.put(UNFRIEND,         0.05D);
            actionsProbability.put(FOREST_FIRE,      0.05D);
            actionsProbability.put(ADD_PARTITION,    0.05D);
            actionsProbability.put(REMOVE_PARTITION, 0.01D);
            actionsProbability.put(DOWNTIME,         0.025D);

            Analyzer.ACTIONS[] script = new Analyzer.ACTIONS[2001];
            for(int j=0; j<script.length-1; j++) {
                script[j] = getActions(actionsProbability);
            }
            script[script.length-1] = DOWNTIME;

            runScriptedTest(sparMiddleware,    script);
        }
    }


    void runScriptedTest(JabejaMiddleware middleware, Analyzer.ACTIONS[] script) {
        //TODO: more work on the actual assertions
        assertTrue(isMiddlewareInAValidState(middleware));
        for(int i=0; i<script.length; i++) {
            Analyzer.ACTIONS action = script[i];
            if(action == ADD_USER) {
                logger.warn("(" + i + "): " + ADD_USER + ": pre");
                int newUid = middleware.addUser();
                logger.warn("(" + i + "): " + ADD_USER + ": " + newUid);
                assertTrue(isMiddlewareInAValidState(middleware));
            }
            if(action == REMOVE_USER) {
                int badId = ProbabilityUtils.getRandomElement(middleware.getUserIds());
                logger.warn("(" + i + "): " + REMOVE_USER + ": " + badId);
                middleware.removeUser(badId);
                assertTrue(isMiddlewareInAValidState(middleware));
            }
            if(action == BEFRIEND) {
                int[] nonfriendship = getNonFriendship(middleware.getFriendships());
                int uid1 = nonfriendship[0];
                int uid2 = nonfriendship[1];

                logger.warn("(" + i + "): " + BEFRIEND + ": " + uid1 + "<->" + uid2);
                middleware.befriend(uid1, uid2);
                assertTrue(isMiddlewareInAValidState(middleware));
            }
            if(action == UNFRIEND) {
                int[] friendship = getFriendship(middleware.getFriendships());
                logger.warn("(" + i + "): " + UNFRIEND + ": " + friendship[0] + "<->" + friendship[1]);
                middleware.unfriend(friendship[0], friendship[1]);
                assertTrue(isMiddlewareInAValidState(middleware));
            }
            if(action == FOREST_FIRE) {
                ForestFireGenerator generator = new ForestFireGenerator(.34f, .34f, new TreeMap<Integer, Set<Integer>>(copyMapSet(middleware.getFriendships())));
                Set<Integer> newUsersFriends = generator.run();
                int newUid = generator.getV();
                logger.warn("(" + i + "): " + FOREST_FIRE + ": " + newUid + "<->" + newUsersFriends);
                middleware.addUser(new User(newUid));
                assertTrue(isMiddlewareInAValidState(middleware));
                for(Integer friend : newUsersFriends) {
                    middleware.befriend(newUid, friend);
                }
                assertTrue(isMiddlewareInAValidState(middleware));
            }
            if(action == ADD_PARTITION) {
                Set<Integer> oldPids = new HashSet<Integer>(middleware.getPartitionIds());
                logger.warn("(" + i + "): " + ADD_PARTITION + ": pre");
                int newPid = middleware.addPartition();
                logger.warn("(" + i + "): " + ADD_PARTITION + ": " + newPid);
                assertTrue(isMiddlewareInAValidState(middleware));
                Set<Integer> newPids = new HashSet<Integer>(middleware.getPartitionIds());
                newPids.removeAll(oldPids);
                assertTrue(newPids.size() == 1);
                assertTrue(newPids.contains(newPid));
            }
            if(action == REMOVE_PARTITION) {
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                int badId = ProbabilityUtils.getRandomElement(middleware.getPartitionIds());
                logger.warn("(" + i + "): " + REMOVE_PARTITION + ": " + badId);
                middleware.removePartition(badId);
                assertTrue(isMiddlewareInAValidState(middleware));
                pids.removeAll(middleware.getPartitionIds());
                assertTrue(pids.size() == 1);
                assertTrue(pids.contains(badId));
            }
            if(action == DOWNTIME) {
                logger.warn("(" + i + "): " + DOWNTIME);
                middleware.broadcastDowntime();
                assertTrue(isMiddlewareInAValidState(middleware));
            }
        }
    }

    static Analyzer.ACTIONS getActions(Map<Analyzer.ACTIONS, Double> actionsProbability) {
        double random = Math.random();

        for(Analyzer.ACTIONS action : Analyzer.ACTIONS.values()) {
            if(random < actionsProbability.get(action)) {
                return action;
            }
            random -= actionsProbability.get(action);
        }

        return DOWNTIME;
    }

    private JabejaManager initJabejaManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) throws Exception {
        return JabejaTestUtils.initGraph(1.5f, 2f, 0.2f, 9, partitions, friendships);
    }

    private JabejaMiddleware initJabejaMiddleware(JabejaManager manager) {
        return new JabejaMiddleware(manager);
    }

    private static boolean isMiddlewareInAValidState(JabejaMiddleware middleware) {
        boolean valid = true;
        Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
        Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
        valid &= (middleware.getNumberOfPartitions().intValue() == pids.size());
        valid &= (middleware.getNumberOfUsers().intValue()      == uids.size());

        Map<Integer, Set<Integer>> partitions  = middleware.getPartitionToUserMap();
        Map<Integer, Set<Integer>> friendships = middleware.getFriendships();

        valid &= (pids.equals(partitions.keySet()));
        valid &= (uids.equals(friendships.keySet()));

        for(int uid : uids) {
            try {
                valid &= (findKeysForUser(partitions, uid).size() == 1);
            } catch(AssertionError e) {
                throw e;
            }
        }

        Set<Integer> allMastersSeen = new HashSet<Integer>();
        for(int pid : partitions.keySet()) {
            allMastersSeen.addAll(partitions.get(pid));
        }
        allMastersSeen.removeAll(middleware.getUserIds());
        valid &= (allMastersSeen.isEmpty());

        Set<Integer> allFriendsSeen = new HashSet<Integer>();
        for(int pid : friendships.keySet()) {
            allFriendsSeen.addAll(friendships.get(pid));
        }
        allFriendsSeen.removeAll(middleware.getUserIds());
        valid &= (allFriendsSeen.isEmpty());

        return valid;
    }

}
