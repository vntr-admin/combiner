package io.vntr.sparmes;

import io.vntr.Analyzer;
import io.vntr.ForestFireGenerator;
import io.vntr.TestUtils;
import io.vntr.User;
import io.vntr.utils.ProbabilityUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.*;

import static io.vntr.Analyzer.ACTIONS.*;
import static io.vntr.Analyzer.ACTIONS.ADD_PARTITION;
import static io.vntr.Analyzer.ACTIONS.REMOVE_PARTITION;
import static io.vntr.TestUtils.*;
import static io.vntr.TestUtils.copyMapSet;
import static io.vntr.TestUtils.getFriendship;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by robertlindquist on 10/9/16.
 */
public class SparmesAnalyzer {
    final static Logger logger = Logger.getLogger(SparmesAnalyzer.class);

//    @Test
    public void stressTest() throws Exception {
        for(int i=0; i<1000; i++) {
            int numUsers = 1000;//500 + (int) (Math.random() * 2000);
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

            SparmesManager SparmesManager = initSparmesManager(friendships, partitions, replicas);
            SparmesMiddleware SparmesMiddleware = initSparmesMiddleware(SparmesManager);

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

            runScriptedTest(SparmesMiddleware,    script);
        }
    }

    void runScriptedTest(SparmesMiddleware middleware, Analyzer.ACTIONS[] script) {
        //TODO: more work on the actual assertions, especially replica-specific ones
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
                Map<Integer, Set<Integer>> oldPartitions = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Map<Integer, Set<Integer>> friendships = copyMapSet(middleware.getFriendships());
                Set<Integer> oldUids = new HashSet<Integer>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                int badId = ProbabilityUtils.getRandomElement(middleware.getUserIds());
                logger.warn("(" + i + "): " + REMOVE_USER + ": " + badId);
                middleware.removeUser(badId);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(pids, middleware.getPartitionIds());
                oldUids.remove(badId);
                assertEquals(middleware.getUserIds(), oldUids);
                friendships.remove(badId);
                for(int uid : middleware.getUserIds()) {
                    friendships.get(uid).remove(badId);
                }
                assertEquals(friendships, middleware.getFriendships());
                Map<Integer, Set<Integer>> newPartitions  = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                assertUserRemoved(badId, 2, oldPartitions, oldReplicas, newPartitions, newReplicas);
            }
            if(action == BEFRIEND) {
                Map<Integer, Set<Integer>> oldMasters   = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Map<Integer, Set<Integer>> friendships = copyMapSet(middleware.getFriendships());
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
                friendships.get(uid1).add(uid2);
                friendships.get(uid2).add(uid1);
                assertEquals(friendships, middleware.getFriendships());
                Map<Integer, Set<Integer>> newMasters   = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                Map<Integer, Set<Integer>> newFriendships = middleware.getFriendships();
//                assertProperBefriending(uid1, uid2, oldMasters, oldReplicas, oldFriendships, newMasters, newReplicas, newFriendships); TODO: implement this
            }
            if(action == UNFRIEND) {
                Map<Integer, Set<Integer>> oldMasters   = copyMapSet(middleware.getPartitionToUserMap());
                Map<Integer, Set<Integer>> oldReplicas   = copyMapSet(middleware.getPartitionToReplicaMap());
                Map<Integer, Set<Integer>> oldFriendships = copyMapSet(middleware.getFriendships());
                Map<Integer, Set<Integer>> friendships = copyMapSet(middleware.getFriendships());
                Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
                Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
                int[] friendship = getFriendship(middleware.getFriendships());
                logger.warn("(" + i + "): " + UNFRIEND + ": " + friendship[0] + "<->" + friendship[1]);
                middleware.unfriend(friendship[0], friendship[1]);
                assertTrue(isMiddlewareInAValidState(middleware));
                assertEquals(uids, middleware.getUserIds());
                assertEquals(pids, middleware.getPartitionIds());
                friendships.get(friendship[0]).remove(friendship[1]);
                friendships.get(friendship[1]).remove(friendship[0]);
                assertEquals(friendships, middleware.getFriendships());
                Map<Integer, Set<Integer>> newMasters   = middleware.getPartitionToUserMap();
                Map<Integer, Set<Integer>> newReplicas    = middleware.getPartitionToReplicaMap();
                Map<Integer, Set<Integer>> newFriendships = middleware.getFriendships();
                assertProperUnfriending(friendship[0], friendship[1], oldMasters, oldReplicas, oldFriendships, newMasters, newReplicas, newFriendships);
            }
            if(action == FOREST_FIRE) {
                Map<Integer, Set<Integer>> friendships = copyMapSet(middleware.getFriendships());
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
                friendships.put(newUid, newUsersFriends);
                for(int friendId : newUsersFriends) {
                    friendships.get(friendId).add(newUid);
                }
//                assertEquals(friendships, middleware.getFriendships());

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
            if(action == REMOVE_PARTITION && false) {
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
            if(action == DOWNTIME && false) {
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

    private SparmesManager initSparmesManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SparmesTestUtils.initGraph(2, 1.2f, true, partitions, friendships, replicas);
    }

    private SparmesMiddleware initSparmesMiddleware(SparmesManager manager) {
        return new SparmesMiddleware(manager);
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

    private static boolean isMiddlewareInAValidState(SparmesMiddleware middleware) {
        //TODO: add back in the replica-specific stuff
        boolean valid = true;
        Set<Integer> pids = new HashSet<Integer>(middleware.getPartitionIds());
        Set<Integer> uids = new HashSet<Integer>(middleware.getUserIds());
        valid &= (middleware.getNumberOfPartitions().intValue() == pids.size());
        valid &= (middleware.getNumberOfUsers().intValue()      == uids.size());

        Map<Integer, Set<Integer>> partitions  = middleware.getPartitionToUserMap();
        Map<Integer, Set<Integer>> friendships = middleware.getFriendships();
        Map<Integer, Set<Integer>> replicas = middleware.getPartitionToReplicaMap();

        valid &= (pids.equals(partitions.keySet()));
        valid &= (uids.equals(friendships.keySet()));
        valid &= (pids.equals(replicas.keySet()));

        for(int uid : uids) {
            try {
                valid &= (findKeysForUser(partitions, uid).size() == 1);
                valid &= findKeysForUser(replicas, uid).size() >= middleware.manager.getMinNumReplicas();
            } catch(AssertionError e) {
                throw e;
            }
        }

        for(int uid1 : friendships.keySet()) {
            for(int uid2 : friendships.get(uid1)) {
                int pid1 = findKeysForUser(partitions, uid1).iterator().next();
                int pid2 = findKeysForUser(partitions, uid2).iterator().next();
                if(pid1 != pid2) {
                    //If they aren't colocated, they have replicas in each other's partitions
                    valid &= (findKeysForUser(replicas, uid1).contains(pid2));
                    valid &= (findKeysForUser(replicas, uid2).contains(pid1));
                }
            }
        }

        //Assert that replicas are consistent with the master in everything except partitionId
        for(int uid : friendships.keySet()) {
            SparmesUser master = middleware.manager.getUserMasterById(uid);
            Set<Integer> replicaPids = findKeysForUser(replicas, uid);
            for(int replicaPid : replicaPids) {
                SparmesUser replica = middleware.manager.getPartitionById(replicaPid).getReplicaById(uid);
                assertEquals(master.getId(), replica.getId());
                assertEquals(master.getFriendIDs(), replica.getFriendIDs());
                assertEquals(master.getMasterPartitionId(), replica.getMasterPartitionId());
                assertEquals(master.getReplicaPartitionIds(), replica.getReplicaPartitionIds());
                assertEquals(master.getPartitionId(), master.getMasterPartitionId());
                assertEquals(replica.getPartitionId(), (Integer) replicaPid);
            }
        }

        Set<Integer> allMastersSeen = new HashSet<Integer>();
        for(int pid : partitions.keySet()) {
            allMastersSeen.addAll(partitions.get(pid));
        }
        allMastersSeen.removeAll(middleware.getUserIds());
        valid &= (allMastersSeen.isEmpty());

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
        valid &= (allFriendsSeen.isEmpty());

        for(int uid : uids) {
            int pid = findKeysForUser(partitions, uid).iterator().next();
            Set<Integer> reps = findKeysForUser(replicas, uid);
            if(reps.contains(pid)) {
                logger.warn(uid + " has a master in " + pid + " and replicas in " + reps);
                valid = false;
            }
        }

        return valid;
    }

    private static void assertUserRemoved(int uid, int minNumReplicas, Map<Integer, Set<Integer>> oldPartitions, Map<Integer, Set<Integer>> oldReplicas, Map<Integer, Set<Integer>> newPartitions, Map<Integer, Set<Integer>> newReplicas) {
        assertTrue(findKeysForUser(oldPartitions,  uid).size() == 1);
        assertTrue(findKeysForUser(oldReplicas,    uid).size() >= minNumReplicas);
        assertTrue(findKeysForUser(newPartitions,  uid).size() == 0);
        assertTrue(findKeysForUser(newReplicas,    uid).size() == 0);
    }

    private static void assertProperUnfriending(int uid1, int uid2, Map<Integer, Set<Integer>> oldMasters, Map<Integer, Set<Integer>> oldReplicas, Map<Integer, Set<Integer>> oldFriendships, Map<Integer, Set<Integer>> newMasters, Map<Integer, Set<Integer>> newReplicas, Map<Integer, Set<Integer>> newFriendships) {
        int minUid = Math.min(uid1, uid2);
        int maxUid = Math.max(uid1, uid2);
        assertTrue(oldFriendships.get(minUid).contains(maxUid));
        assertFalse(newFriendships.get(minUid).contains(maxUid));

        int minPid = findKeysForUser(newMasters, minUid).iterator().next();
        int maxPid = findKeysForUser(newMasters, maxUid).iterator().next();

        if(minPid != maxPid) {
            assertTrue(oldReplicas.get(minPid).contains(maxUid));
            assertTrue(oldReplicas.get(maxPid).contains(minUid));

            Map<Integer, Set<Integer>> bidirectionalFriendships = ProbabilityUtils.generateBidirectionalFriendshipSet(newFriendships);

            boolean minUidHasExtraReplicas = findKeysForUser(oldReplicas, minUid).size() > 2;
            boolean maxUidHasExtraReplicas = findKeysForUser(oldReplicas, maxUid).size() > 2;

            Set<Integer> friendsOfMaxUidOnMinPid = new HashSet<Integer>(bidirectionalFriendships.get(maxUid));
            friendsOfMaxUidOnMinPid.retainAll(oldMasters.get(minPid));
            friendsOfMaxUidOnMinPid.remove(minUid);
            boolean maxUidHasOtherFriendsOnMinPid = !friendsOfMaxUidOnMinPid.isEmpty();

            Set<Integer> friendsOfMinUidOnMaxPid = new HashSet<Integer>(bidirectionalFriendships.get(minUid));
            friendsOfMinUidOnMaxPid.retainAll(oldMasters.get(maxPid));
            friendsOfMinUidOnMaxPid.remove(maxUid);
            boolean minUidHasOtherFriendsOnMaxPid = !friendsOfMinUidOnMaxPid.isEmpty();

            boolean minUidShouldHaveReplicaOnMaxPid = !minUidHasExtraReplicas || minUidHasOtherFriendsOnMaxPid;
            boolean maxUidShouldHaveReplicaOnMinPid = !maxUidHasExtraReplicas || maxUidHasOtherFriendsOnMinPid;
            assertTrue(maxUidShouldHaveReplicaOnMinPid == newReplicas.get(minPid).contains(maxUid));
            assertTrue(minUidShouldHaveReplicaOnMaxPid == newReplicas.get(maxPid).contains(minUid));
        }
    }
}
