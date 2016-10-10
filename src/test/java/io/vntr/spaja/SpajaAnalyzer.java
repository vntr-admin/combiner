package io.vntr.spaja;

import io.vntr.Analyzer;
import io.vntr.ForestFireGenerator;
import io.vntr.TestUtils;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.*;

import static io.vntr.Analyzer.ACTIONS.*;
import static io.vntr.Analyzer.ACTIONS.DOWNTIME;
import static io.vntr.TestUtils.*;
import static io.vntr.TestUtils.copyMapSet;
import static io.vntr.TestUtils.getFriendship;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 10/9/16.
 */
public class SpajaAnalyzer {
    final static Logger logger = Logger.getLogger(SpajaAnalyzer.class);

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
            Map<Integer, Set<Integer>> replicas = TestUtils.getInitialReplicasObeyingKReplication(2, partitions, friendships);

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

            SpajaManager SpajaManager = initSpajaManager(friendships, partitions, replicas);
            SpajaMiddleware SpajaMiddleware = initSpajaMiddleware(SpajaManager);

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

            runScriptedTest(SpajaMiddleware,    script);
        }
    }

    void runScriptedTest(SpajaMiddleware middleware, Analyzer.ACTIONS[] script) {
        //TODO: more work on the actual assertions, especially replica-specific ones
        isMiddlewareInAValidState(middleware, 2);
        for(int i=0; i<script.length; i++) {
            Analyzer.ACTIONS action = script[i];
            if(action == ADD_USER) {
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                Set<Integer> oldUids = new HashSet<Integer>(middleware.getUserIds());
                logger.warn("(" + i + "): " + ADD_USER + ": pre");
                int newUid = middleware.addUser();
                logger.warn("(" + i + "): " + ADD_USER + ": " + newUid);
                isMiddlewareInAValidState(middleware, 2);
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
                isMiddlewareInAValidState(middleware, 2);
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
                isMiddlewareInAValidState(middleware, 2);
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
                isMiddlewareInAValidState(middleware, 2);
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
                isMiddlewareInAValidState(middleware, 2);
                for(Integer friend : newUsersFriends) {
                    middleware.befriend(newUid, friend);
                }
                isMiddlewareInAValidState(middleware, 2);
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
                isMiddlewareInAValidState(middleware, 2);
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
                isMiddlewareInAValidState(middleware, 2);
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
                isMiddlewareInAValidState(middleware, 2);
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

    private SpajaManager initSpajaManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SpajaTestUtils.initGraph(2, 1.5f, 2f, 0.2f, 9, partitions, friendships, replicas);
    }

    private SpajaMiddleware initSpajaMiddleware(SpajaManager manager) {
        return new SpajaMiddleware(manager);
    }

    private static void isMiddlewareInAValidState(SpajaMiddleware middleware, int minNumReplicas) {
        //TODO: add back in the replica-specific stuff
        Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
        Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
        assertTrue(middleware.getNumberOfPartitions().intValue() == pids.size());
        assertTrue(middleware.getNumberOfUsers().intValue()      == uids.size());

        Map<Integer, Set<Integer>> partitions  = middleware.getPartitionToUserMap();
        Map<Integer, Set<Integer>> replicas    = middleware.getPartitionToReplicaMap();
        Map<Integer, Set<Integer>> friendships = middleware.getFriendships();

        assertTrue(pids.equals(partitions.keySet()));
        assertTrue(pids.equals(replicas.keySet()));
        assertTrue(uids.equals(friendships.keySet()));

        for(int uid : uids) {
            assertTrue(findKeysForUser(partitions, uid).size() == 1);
            assertTrue(findKeysForUser(replicas, uid).size() >= minNumReplicas);
        }

        for(int uid1 : friendships.keySet()) {
            for(int uid2 : friendships.get(uid1)) {
                int pid1 = findKeysForUser(partitions, uid1).iterator().next();
                int pid2 = findKeysForUser(partitions, uid2).iterator().next();
                if(pid1 != pid2) {
                    //If they aren't colocated, they have replicas in each other's partitions
                    assertTrue(findKeysForUser(replicas, uid1).contains(pid2));
                    assertTrue(findKeysForUser(replicas, uid2).contains(pid1));
                }
            }
        }

        //Assert that replicas are consistent with the master in everything except partitionId
        for(int uid : friendships.keySet()) {
            SpajaUser master = middleware.manager.getUserMasterById(uid);
            Set<Integer> replicaPids = findKeysForUser(replicas, uid);
            for(int replicaPid : replicaPids) {
                SpajaUser replica = middleware.manager.getPartitionById(replicaPid).getReplicaById(uid);
                assertTrue(master.getId().equals(replica.getId()));
                assertTrue(master.getFriendIDs().equals(replica.getFriendIDs()));
                assertTrue(master.getMasterPartitionId().equals(replica.getMasterPartitionId()));
                assertTrue(master.getReplicaPartitionIds().equals(replica.getReplicaPartitionIds()));
                assertTrue(master.getPartitionId().equals(master.getMasterPartitionId()));
                assertTrue(replica.getPartitionId().equals((Integer) replicaPid));
            }
        }

        Set<Integer> allMastersSeen = new HashSet<Integer>();
        for(int pid : partitions.keySet()) {
            allMastersSeen.addAll(partitions.get(pid));
        }
        allMastersSeen.removeAll(middleware.getUserIds());
        assertTrue(allMastersSeen.isEmpty());

        Set<Integer> allReplicasSeen = new HashSet<Integer>();
        for(int pid : replicas.keySet()) {
            allReplicasSeen.addAll(replicas.get(pid));
        }
        allReplicasSeen.removeAll(middleware.getUserIds());
        assertTrue(allReplicasSeen.isEmpty());

        Set<Integer> allFriendsSeen = new HashSet<Integer>();
        for(int pid : friendships.keySet()) {
            allFriendsSeen.addAll(friendships.get(pid));
        }
        allFriendsSeen.removeAll(middleware.getUserIds());
        assertTrue(allFriendsSeen.isEmpty());
    }
}
