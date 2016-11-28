package io.vntr.hermes;

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
 * Created by robertlindquist on 10/9/16.
 */
public class HermesAnalyzer {
    final static Logger logger = Logger.getLogger(HermesAnalyzer.class);

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

            logger.warn("numUsers:          " + numUsers);
            logger.warn("numGroups:         " + numGroups);
            logger.warn("groupProb:         " + groupProb);
            logger.warn("friendProb:        " + friendProb);
            logger.warn("usersPerPartition: " + usersPerPartition);
            logger.warn("numFriendships:    " + numFriendships);
            logger.warn("assortivity:       " + assortivity);
            logger.warn("friendships:       " + friendships);
            logger.warn("partitions:        " + partitions);

            HermesManager hermesManager = initHermesManager(friendships, partitions);
            HermesMiddleware hermesMiddleware = initHermesMiddleware(hermesManager);

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

            runScriptedTest(hermesMiddleware,    script);
        }
    }


    void runScriptedTest(HermesMiddleware middleware, Analyzer.ACTIONS[] script) {
        //TODO: more work on the actual assertions
        assertTrue(isMiddlewareInAValidState(middleware));
        for(int i=0; i<script.length; i++) {
            Analyzer.ACTIONS action = script[i];
            if(action == ADD_USER) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                Set<Integer> oldUids = new HashSet<Integer>(middleware.getUserIds());
                logger.warn("(" + i + "): " + ADD_USER + ": pre");
                int newUid = middleware.addUser();
                logger.warn("(" + i + "): " + ADD_USER + ": " + newUid);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(pids, middleware.getPartitionIds());
                oldUids.add(newUid);
                assertEquals(middleware.getUserIds(), oldUids);
                oldFriendships.put(newUid, Collections.<Integer>emptySet());
                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == REMOVE_USER) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> oldUids = new HashSet<Integer>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                int badId = ProbabilityUtils.getRandomElement(middleware.getUserIds());
                logger.warn("(" + i + "): " + REMOVE_USER + ": " + badId);
                middleware.removeUser(badId);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(pids, middleware.getPartitionIds());
                oldUids.remove(badId);
                assertEquals(middleware.getUserIds(), oldUids);
                oldFriendships.remove(badId);
                for(int uid : middleware.getUserIds()) {
                    oldFriendships.get(uid).remove(badId);
                }
                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == BEFRIEND) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                int[] nonfriendship = getNonFriendship(middleware.getFriendships());
                int uid1 = nonfriendship[0];
                int uid2 = nonfriendship[1];

                logger.warn("(" + i + "): " + BEFRIEND + ": " + uid1 + "<->" + uid2);
                middleware.befriend(uid1, uid2);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(pids, middleware.getPartitionIds());
                oldFriendships.get(uid1).add(uid2);
                oldFriendships.get(uid2).add(uid1);
                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == UNFRIEND) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                int[] friendship = getFriendship(middleware.getFriendships());
                logger.warn("(" + i + "): " + UNFRIEND + ": " + friendship[0] + "<->" + friendship[1]);
                middleware.unfriend(friendship[0], friendship[1]);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(pids, middleware.getPartitionIds());
                oldFriendships.get(friendship[0]).remove(friendship[1]);
                oldFriendships.get(friendship[1]).remove(friendship[0]);
                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == FOREST_FIRE) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
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
                uids.add(newUid);
                assertEquals(uids, middleware.getUserIds());
                assertEquals(pids, middleware.getPartitionIds());
                oldFriendships.put(newUid, newUsersFriends);
                for(int friendId : newUsersFriends) {
                    oldFriendships.get(friendId).add(newUid);
                }
//                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == ADD_PARTITION) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
                Set<Integer> oldPids = new HashSet<Integer>(middleware.getPartitionIds());
                logger.warn("(" + i + "): " + ADD_PARTITION + ": pre");
                int newPid = middleware.addPartition();
                logger.warn("(" + i + "): " + ADD_PARTITION + ": " + newPid);
                assertTrue(isMiddlewareInAValidState(middleware));
                Set<Integer> newPids = new HashSet<Integer>(middleware.getPartitionIds());
                newPids.removeAll(oldPids);
                assertTrue(newPids.size() == 1);
                assertTrue(newPids.contains(newPid));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == REMOVE_PARTITION) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                int badId = ProbabilityUtils.getRandomElement(middleware.getPartitionIds());
                logger.warn("(" + i + "): " + REMOVE_PARTITION + ": " + badId);
                middleware.removePartition(badId);
                assertTrue(isMiddlewareInAValidState(middleware));
                pids.removeAll(middleware.getPartitionIds());
                assertTrue(pids.size() == 1);
                assertTrue(pids.contains(badId));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(oldFriendships, middleware.getFriendships());
            }
            if(action == DOWNTIME) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                logger.warn("(" + i + "): " + DOWNTIME);
                middleware.broadcastDowntime();
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(pids, middleware.getPartitionIds());
                assertEquals(oldFriendships, middleware.getFriendships());
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

    private HermesManager initHermesManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) throws Exception {
        return HermesTestUtils.initGraph(1.3f, 3, 0.03f, partitions, friendships);
    }

    private HermesMiddleware initHermesMiddleware(HermesManager manager) {
        return new HermesMiddleware(manager, 1.3f);
    }

    private static boolean isMiddlewareInAValidState(HermesMiddleware middleware) {
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

        double gamma = middleware.getGamma();
        double avgSize = ((double) middleware.getNumberOfUsers()) / (middleware.getNumberOfPartitions());
        int overloaded = (int) (gamma * avgSize) + 1;

        double maxImbalance = Double.MIN_VALUE;
        double minImbalance = Double.MAX_VALUE;
        for(int pid : partitions.keySet()) {
            double imbalance = ((double) partitions.get(pid).size()) / (avgSize);
            if(imbalance < minImbalance) {
                minImbalance = imbalance;
            }
            if(imbalance > maxImbalance) {
                maxImbalance = imbalance;
            }
        }
        if(maxImbalance > gamma) {
            logger.warn("Imbalance range: " + minImbalance + " - " + maxImbalance);
        }

        return valid;
    }

}
