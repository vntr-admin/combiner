package io.vntr.spj2;

import java.util.*;

import io.vntr.RepUser;
import org.junit.Test;

import static io.vntr.TestUtils.initSet;
import static io.vntr.spj2.BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL;
import static io.vntr.spj2.BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
import static io.vntr.spj2.BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;
import static java.util.Collections.singleton;
import static org.junit.Assert.*;

public class SpJ2BefriendingStrategyTest {
    private static class Scenario {
        public final SpJ2Manager manager;
        public final RepUser alpha;
        public final RepUser beta;
        public final RepUser aleph;
        public final RepUser gamma;

        public Scenario(SpJ2Manager manager, RepUser alpha, RepUser beta, RepUser aleph, RepUser gamma) {
            this.manager = manager;
            this.alpha = alpha;
            this.beta = beta;
            this.aleph = aleph;
            this.gamma = gamma;
        }
    }

    /**
     * alpha has no replica in B, beta has no replica in A, alpha and beta have no friends, alpha and beta both exceed minimum redundancy
     */
    private static Scenario getScenario1() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = manager.getPids().iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPids().contains(partitionAId)) {
            Integer newReplicaLocation = SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        //ensure that A and B exceed redundancy requirements
        Set<Integer> partitionsWithoutAlphaOrBetaMasters = new HashSet<>(manager.getPids());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Integer extraAlphaReplicaLocation = null;
        Integer extraBetaReplicaLocation = null;
        for (Integer partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPids().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPids().contains(partitionId)) {
                extraBetaReplicaLocation = partitionId;
            }
        }
        assertNotNull(extraAlphaReplicaLocation);
        assertNotNull(extraBetaReplicaLocation);
        manager.addReplica(alpha, extraAlphaReplicaLocation);
        manager.addReplica(beta, extraBetaReplicaLocation);

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has replica in B, beta has no replica in A, alpha and beta have no friends, alpha and beta exceed minimum redundancy
     */
    private static Scenario getScenario2() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = manager.getPids().iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = alpha.getReplicaPids().iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPids().contains(partitionAId)) {
            Integer newReplicaLocation = SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        //ensure that A and B exceed redundancy requirements
        Set<Integer> partitionsWithoutAlphaOrBetaMasters = new HashSet<>(manager.getPids());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Integer extraAlphaReplicaLocation = null;
        Integer extraBetaReplicaLocation = null;
        for (Integer partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPids().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPids().contains(partitionId)) {
                extraBetaReplicaLocation = partitionId;
            }
        }
        assertNotNull(extraAlphaReplicaLocation);
        assertNotNull(extraBetaReplicaLocation);
        manager.addReplica(alpha, extraAlphaReplicaLocation);
        manager.addReplica(beta, extraBetaReplicaLocation);

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has replica in B, beta has no replica in A, alpha and beta have no friends, alpha does not exceed minimum redundancy, but beta does
     */
    private static Scenario getScenario3() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = manager.getPids().iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = alpha.getReplicaPids().iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPids().contains(partitionAId)) {
            Integer newReplicaLocation = SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        Set<Integer> locationsWhereWeCouldReplicateBeta = new HashSet<>(SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()));
        locationsWhereWeCouldReplicateBeta.remove(partitionAId);
        manager.addReplica(beta, locationsWhereWeCouldReplicateBeta.iterator().next());

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has replica in B, beta has replica in A, alpha and beta have no friends, alpha and beta both exceed minimum redundancy
     */
    private static Scenario getScenario4() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = manager.getPids().iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = alpha.getReplicaPids().iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPids().contains(partitionAId)) {
            Integer betaReplicaToNix = beta.getReplicaPids().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        Set<Integer> partitionsWithoutAlphaOrBetaMasters = new HashSet<>(manager.getPids());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Integer extraAlphaReplicaLocation = null;
        Integer extraBetaReplicaLocation = null;
        for (Integer partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPids().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPids().contains(partitionId)) {
                extraBetaReplicaLocation = partitionId;
            }
        }
        assertNotNull(extraAlphaReplicaLocation);
        assertNotNull(extraBetaReplicaLocation);
        manager.addReplica(alpha, extraAlphaReplicaLocation);
        manager.addReplica(beta, extraBetaReplicaLocation);
        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has replica in B, beta has replica in A, alpha and beta have no friends, beta exceeds minimum redundancy but alpha does not
     */
    private static Scenario getScenario5() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = manager.getPids().iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = alpha.getReplicaPids().iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPids().contains(partitionAId)) {
            Integer betaReplicaToNix = beta.getReplicaPids().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        manager.addReplica(beta, SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next());

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has replica in B, beta has replica in A, alpha and beta have no friends, alpha exceeds minimum redundancy but beta does not
     */
    private static Scenario getScenario6() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = manager.getPids().iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = alpha.getReplicaPids().iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPids().contains(partitionAId)) {
            Integer betaReplicaToNix = beta.getReplicaPids().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        manager.addReplica(alpha, SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next());

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has replica in B, beta has replica in A, alpha and beta have no friends, neither alpha nor beta exceeds minimum redundancy
     */
    private static Scenario getScenario7() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = manager.getPids().iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = alpha.getReplicaPids().iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPids().contains(partitionAId)) {
            Integer betaReplicaToNix = beta.getReplicaPids().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has no replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has a replica in B, no other friendships exist, both alpha and beta exceed minimum redundancy
     */
    private static Scenario getScenario8() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPids().contains(partitionAId)) {
            Integer newReplicaLocation = SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        Set<Integer> partitionsWithoutAlphaOrBetaMasters = new HashSet<>(manager.getPids());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Integer extraAlphaReplicaLocation = null;
        Integer extraBetaReplicaLocation = null;
        for (Integer partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPids().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPids().contains(partitionId)) {
                extraBetaReplicaLocation = partitionId;
            }
        }
        assertNotNull(extraAlphaReplicaLocation);
        assertNotNull(extraBetaReplicaLocation);
        manager.addReplica(alpha, extraAlphaReplicaLocation);
        manager.addReplica(beta, extraBetaReplicaLocation);

        Set<Integer> otherUsersInA = new HashSet<>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());
        RepUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());
        manager.befriend(alpha, aleph);

        //ensure aleph has a replica in B
        if (!aleph.getReplicaPids().contains(partitionBId)) {
            Integer partitionForAlephToLeave = aleph.getReplicaPids().iterator().next();
            manager.removeReplica(aleph, partitionForAlephToLeave);
            manager.addReplica(aleph, partitionBId);
        }

        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has no replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has no replica in B, no other friendships exist, both alpha and beta exceed minimum redundancy
     */
    private static Scenario getScenario9() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPids().contains(partitionAId)) {
            Integer newReplicaLocation = SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        Set<Integer> partitionsWithoutAlphaOrBetaMasters = new HashSet<>(manager.getPids());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Integer extraAlphaReplicaLocation = null;
        Integer extraBetaReplicaLocation = null;
        for (Integer partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPids().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPids().contains(partitionId)) {
                extraBetaReplicaLocation = partitionId;
            }
        }
        assertNotNull(extraAlphaReplicaLocation);
        assertNotNull(extraBetaReplicaLocation);
        manager.addReplica(alpha, extraAlphaReplicaLocation);
        manager.addReplica(beta, extraBetaReplicaLocation);

        Set<Integer> otherUsersInA = new HashSet<>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());
        RepUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());
        manager.befriend(alpha, aleph);

        //ensure aleph doesn't have a replica in B
        if (aleph.getReplicaPids().contains(partitionBId)) {
            Integer newPartitionId = SpJ2TestUtils.getPartitionsWithNoPresence(manager, aleph.getId()).iterator().next();
            manager.addReplica(aleph, newPartitionId);
            manager.removeReplica(aleph, partitionBId);
        }

        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has a replica in B, no other friendships exist, alpha exceeds minimum redundancy
     */
    private static Scenario getScenario10() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = alpha.getReplicaPids().iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        Set<Integer> otherUsersInA = new HashSet<>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());

        RepUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());
        manager.befriend(alpha, aleph);

        if (beta.getReplicaPids().contains(partitionAId)) {
            Integer newReplicaLocation = SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        //ensure aleph has a replica in B
        if (!aleph.getReplicaPids().contains(partitionBId)) {
            Integer partitionForAlephToLeave = aleph.getReplicaPids().iterator().next();
            manager.removeReplica(aleph, partitionForAlephToLeave);
            manager.addReplica(aleph, partitionBId);
        }

        Integer extraPartitionForAlphaReplica = SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        manager.addReplica(alpha, extraPartitionForAlphaReplica);

        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has no replica in B, no other friendships exist, alpha exceeds minimum redundancy
     */
    private static Scenario getScenario11() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = alpha.getReplicaPids().iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        Set<Integer> otherUsersInA = new HashSet<>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());

        RepUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());
        manager.befriend(alpha, aleph);

        if (beta.getReplicaPids().contains(partitionAId)) {
            Integer newReplicaLocation = SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        //ensure aleph doesn't have a replica in B
        if (aleph.getReplicaPids().contains(partitionBId)) {
            Integer newPartitionId = SpJ2TestUtils.getPartitionsWithNoPresence(manager, aleph.getId()).iterator().next();
            manager.addReplica(aleph, newPartitionId);
            manager.removeReplica(aleph, partitionBId);
        }
        Integer extraPartitionForAlphaReplica = SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        manager.addReplica(alpha, extraPartitionForAlphaReplica);
        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has no replica in B, beta has replica in A, beta has a friend aleph in A, no other friendships exist, beta exceeds minimum redundancy
     */
    private static Scenario getScenario12() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Set<Integer> otherUsersInA = new HashSet<>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());

        RepUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());

        Integer partitionBId = SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPids().contains(partitionAId)) {
            Integer betaReplicaToNix = beta.getReplicaPids().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        if (!aleph.getReplicaPids().contains(partitionBId)) {
            Integer alephReplicaToNix = aleph.getReplicaPids().iterator().next();
            manager.removeReplica(aleph, alephReplicaToNix);
            manager.addReplica(aleph, partitionBId);
        }

        manager.addReplica(beta, SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next());

        manager.befriend(beta, aleph);

        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has no replica in B, beta has no replica in A, alpha is friends with gamma, no other friendships exist, gamma has replica in B, gamma exceeds minimum redundancy
     */
    private static Scenario getScenario13() {
        //Scenario 13: alpha has no replica in B, beta has no replica in A, alpha is friends with gamma, no other friendships exist, gamma has replica in B, gamma exceeds minimum redundancy
        //Result: numReplicas decreases by 1

        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = manager.getPids().iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Integer partitionBId = SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        Set<Integer> otherPartitions = new HashSet<>(manager.getPids());
        otherPartitions.remove(partitionAId);
        otherPartitions.remove(partitionBId);
        Integer partitionCId = otherPartitions.iterator().next();
        RepUser gamma = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionCId);

        //Effectively, gamma needs to have replicas on precisely partitionA and partitionB, and nowhere else, so we just remove all existing ones and add those two manually, plus one other one for redundancy
        for (Integer gammaReplicaId : new HashSet<>(gamma.getReplicaPids())) {
            manager.removeReplica(gamma, gammaReplicaId);
        }
        manager.addReplica(gamma, partitionAId);
        manager.addReplica(gamma, partitionBId);
        manager.addReplica(gamma, SpJ2TestUtils.getPartitionsWithNoPresence(manager, gamma.getId()).iterator().next());

        if (!alpha.getReplicaPids().contains(partitionCId)) {
            Integer alphaReplicaToNix = alpha.getReplicaPids().iterator().next();
            manager.removeReplica(alpha, alphaReplicaToNix);
            manager.addReplica(alpha, partitionCId);
        }

        manager.befriend(alpha, gamma);

        return new Scenario(manager, alpha, beta, null, gamma);
    }

    /**
     * alpha has no replica in B, beta has no replica in A, alpha is friends with gamma, gamma is friends with aleph in A, no other friendships exist, gamma has no replica in B
     */
    private static Scenario getScenario14() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();

        Integer partitionAId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Set<Integer> otherUsersInA = new HashSet<>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());

        RepUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());

        Integer partitionBId = SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        Set<Integer> otherPartitions = new HashSet<>(manager.getPids());
        otherPartitions.remove(partitionAId);
        otherPartitions.remove(partitionBId);
        Integer partitionCId = otherPartitions.iterator().next();
        RepUser gamma = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionCId);

        //Since gamma and alpha are friends, they need replicas on each other's partitions.
        //Same with gamma and aleph, though the above will already insure gamma has a replica on aleph's partition
        if (!gamma.getReplicaPids().contains(partitionAId)) {
            Integer partitionToDitch = gamma.getReplicaPids().iterator().next();
            manager.removeReplica(gamma, partitionToDitch);
            manager.addReplica(gamma, partitionAId);
        }
        if (!alpha.getReplicaPids().contains(partitionCId)) {
            Integer partitionToDitch = alpha.getReplicaPids().iterator().next();
            manager.removeReplica(alpha, partitionToDitch);
            manager.addReplica(alpha, partitionCId);
        }
        if (!aleph.getReplicaPids().contains(partitionCId)) {
            Integer partitionToDitch = aleph.getReplicaPids().iterator().next();
            manager.removeReplica(aleph, partitionToDitch);
            manager.addReplica(aleph, partitionCId);
        }

        //gamma should not have a replica on B
        if (gamma.getReplicaPids().contains(partitionBId)) {
            Integer partitionToAddGamma = SpJ2TestUtils.getPartitionsWithNoPresence(manager, gamma.getId()).iterator().next();
            manager.removeReplica(gamma, partitionBId);
            manager.addReplica(gamma, partitionToAddGamma);
        }

        manager.befriend(alpha, gamma);
        manager.befriend(aleph, gamma);

        return new Scenario(manager, alpha, beta, aleph, gamma);
    }

    @Test
    public void testDetermineStrategy() {
        for(int i=0; i<10; i++) {
            assertEquals(SpJ2BefriendingStrategy.determineStrategy(4, 4, 4, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
            assertEquals(SpJ2BefriendingStrategy.determineStrategy(4, 5, 4, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
            assertEquals(SpJ2BefriendingStrategy.determineStrategy(4, 4, 5, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
            assertEquals(SpJ2BefriendingStrategy.determineStrategy(4, 5, 5, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
        }

        assertEquals(SpJ2BefriendingStrategy.determineStrategy(5, 4, 5, 2, 3), LARGE_TO_SMALL);
        assertEquals(SpJ2BefriendingStrategy.determineStrategy(5, 4, 5, 5, 5), LARGE_TO_SMALL);
        assertEquals(SpJ2BefriendingStrategy.determineStrategy(5, 4, 5, 3, 3), NO_CHANGE);

        assertEquals(SpJ2BefriendingStrategy.determineStrategy(5, 4, 4, 2, 3), LARGE_TO_SMALL);
        assertEquals(SpJ2BefriendingStrategy.determineStrategy(5, 4, 4, 3, 3), NO_CHANGE);

        assertEquals(SpJ2BefriendingStrategy.determineStrategy(5, 5, 4, 3, 2), SMALL_TO_LARGE);
        assertEquals(SpJ2BefriendingStrategy.determineStrategy(5, 5, 4, 5, 5), SMALL_TO_LARGE);
        assertEquals(SpJ2BefriendingStrategy.determineStrategy(5, 5, 4, 3, 3), NO_CHANGE);
    }

    @Test
    public void testCalcNumReplicasNoMovementNoCrossReplicas() {
        //Suppose that user alpha is from partition A with 1 master, while user beta is from a partition B with 2 masters
        //Scenario 1: no replicas of alpha on B, no replicas of beta on A
        //Result: should be 2 replicas

        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        Integer partitionAId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 1).iterator().next();
        Integer partitionBId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SpJ2Partition partitionA = manager.getPartitionById(partitionAId);
        SpJ2Partition partitionB = manager.getPartitionById(partitionBId);
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (alpha.getReplicaPids().contains(partitionBId)) {
            Integer partitionWithoutAlpha = SpJ2TestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
            manager.addReplica(alpha, partitionWithoutAlpha);
            manager.removeReplica(alpha, partitionBId);
        }

        if (beta.getReplicaPids().contains(partitionAId)) {
            Integer partitionWithoutBeta = SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.addReplica(beta, partitionWithoutBeta);
            manager.removeReplica(beta, partitionAId);
        }

        int originalNumReplicas = partitionA.getNumReplicas() + partitionB.getNumReplicas();

        int numReplicasNoMovement = strategy.calcNumReplicasStay(alpha, beta);
        assertTrue(numReplicasNoMovement == originalNumReplicas + 2);
    }

    @Test
    public void testCalcNumReplicasNoMovementOneCrossReplica() {
        //Suppose that user alpha is from partition A with 1 master, while user beta is from a partition B with 2 masters
        //Scenario 2: a replica of alpha on B, no replicas of beta on A
        //Result: should be 1 replica
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        Integer partitionAId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 1).iterator().next();
        Integer partitionBId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SpJ2Partition partitionA = manager.getPartitionById(partitionAId);
        SpJ2Partition partitionB = manager.getPartitionById(partitionBId);
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!alpha.getReplicaPids().contains(partitionBId)) {
            Integer partitionFromWhichToRemoveAlpha = alpha.getReplicaPids().iterator().next();
            manager.removeReplica(alpha, partitionFromWhichToRemoveAlpha);
            manager.addReplica(alpha, partitionBId);
        }

        if (beta.getReplicaPids().contains(partitionAId)) {
            Integer partitionWithoutBeta = SpJ2TestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.addReplica(beta, partitionWithoutBeta);
            manager.removeReplica(beta, partitionAId);
        }

        int originalNumReplicas = partitionA.getNumReplicas() + partitionB.getNumReplicas();

        int numReplicasNoMovement = strategy.calcNumReplicasStay(alpha, beta);
        assertTrue(numReplicasNoMovement == originalNumReplicas + 1);
    }

    @Test
    public void testCalcNumReplicasNoMovementTwoCrossReplicas() {
        //Suppose that user alpha is from partition A with 1 master, while user beta is from a partition B with 2 masters
        //Scenario 3: a replica of alpha on B, a replica  of beta on A
        //Result: should be no replicas
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        Integer partitionAId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 1).iterator().next();
        Integer partitionBId = SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SpJ2Partition partitionA = manager.getPartitionById(partitionAId);
        SpJ2Partition partitionB = manager.getPartitionById(partitionBId);
        RepUser alpha = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionAId);
        RepUser beta = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!alpha.getReplicaPids().contains(partitionBId)) {
            Integer partitionFromWhichToRemoveAlpha = alpha.getReplicaPids().iterator().next();
            manager.removeReplica(alpha, partitionFromWhichToRemoveAlpha);
            manager.addReplica(alpha, partitionBId);
        }

        if (!beta.getReplicaPids().contains(partitionAId)) {
            Integer partitionFromWhichToRemoveBeta = beta.getReplicaPids().iterator().next();
            manager.removeReplica(beta, partitionFromWhichToRemoveBeta);
            manager.addReplica(beta, partitionAId);
        }

        int originalNumReplicas = partitionA.getNumReplicas() + partitionB.getNumReplicas();

        int numReplicasNoMovement = strategy.calcNumReplicasStay(alpha, beta);
        assertTrue(numReplicasNoMovement == originalNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario1() {
        //Scenario 1: alpha has no replica in B, beta has no replica in A, alpha and beta have no friends, alpha and beta both exceed minimum redundancy
        //Result: numReplicas remains unchanged

        Scenario scenario1 = getScenario1();
        SpJ2Manager manager = scenario1.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario1.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario1.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario1.alpha, scenario1.beta);
        assertTrue(curNumReplicas == calculatedNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario2() {
        //Scenario 2: alpha has replica in B, beta has no replica in A, alpha and beta have no friends, alpha and beta exceed minimum redundancy
        //Result: numReplicas decreases by 1

        Scenario scenario2 = getScenario2();
        SpJ2Manager manager = scenario2.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario2.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario2.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario2.alpha, scenario2.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario3() {
        //Scenario 3: alpha has replica in B, beta has no replica in A, alpha and beta have no friends, alpha does not exceed minimum redundancy, but beta does
        //Result: numReplicas unchanged

        Scenario scenario3 = getScenario3();
        SpJ2Manager manager = scenario3.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario3.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario3.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario3.alpha, scenario3.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario4() {
        //Scenario 4: alpha has replica in B, beta has replica in A, alpha and beta have no friends, alpha and beta both exceed minimum redundancy
        //Result: numReplicas decreases by 2

        Scenario scenario4 = getScenario4();
        SpJ2Manager manager = scenario4.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario4.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario4.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario4.alpha, scenario4.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 2);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario5() {
        //Scenario 5: alpha has replica in B, beta has replica in A, alpha and beta have no friends, beta exceeds minimum redundancy but alpha does not
        //Result: numReplicas decreases by 1

        Scenario scenario5 = getScenario5();
        SpJ2Manager manager = scenario5.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario5.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario5.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario5.alpha, scenario5.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario6() {
        //Scenario 6: alpha has replica in B, beta has replica in A, alpha and beta have no friends, alpha exceeds minimum redundancy but beta does not
        //Result: numReplicas decreases by 1

        Scenario scenario6 = getScenario6();
        SpJ2Manager manager = scenario6.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario6.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario6.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario6.alpha, scenario6.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario7() {
        //Scenario 7: alpha has replica in B, beta has replica in A, alpha and beta have no friends, neither alpha nor beta exceeds minimum redundancy
        //Result: numReplicas remains unchanged

        Scenario scenario7 = getScenario7();
        SpJ2Manager manager = scenario7.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario7.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario7.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario7.alpha, scenario7.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario8() {
        //Scenario 8: alpha has no replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has a replica in B, no other friendships exist, both alpha and beta exceed minimum redundancy
        //Result: numReplicas increases by 1

        Scenario scenario8 = getScenario8();
        SpJ2Manager manager = scenario8.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario8.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario8.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario8.alpha, scenario8.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas + 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario9() {
        //Scenario 9: alpha has no replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has no replica in B, no other friendships exist, both alpha and beta exceed minimum redundancy
        //Result: numReplicas increases by 1

        Scenario scenario9 = getScenario9();
        SpJ2Manager manager = scenario9.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario9.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario9.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario9.alpha, scenario9.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas + 2);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario10() {
        //Scenario 10: alpha has replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has a replica in B, no other friendships exist, alpha exceeds minimum redundancy
        //Result: numReplicas remains unchanged

        Integer A = 1;
        Integer B = 2;
        Integer X = 99;
        Integer alpha = 1;
        Integer beta  = 2;
        Integer aleph = 3;
        Integer xi = 112;

        int minNumReplicas = 1;
        Map<Integer, Set<Integer>> partitions = new HashMap<>();
        partitions.put(A, initSet(alpha, aleph));
        partitions.put(B, initSet( beta));
        partitions.put(X, initSet(xi));

        Map<Integer, Set<Integer>> friendships = new HashMap<>();
        friendships.put(alpha, initSet(aleph));
        friendships.put(beta,  Collections.<Integer>emptySet());
        friendships.put(aleph, Collections.<Integer>emptySet());
        friendships.put(xi,    Collections.<Integer>emptySet());

        Map<Integer, Set<Integer>> replicaPartitions = new HashMap<>();
        replicaPartitions.put(A, initSet(xi));
        replicaPartitions.put(B, initSet(alpha, aleph));
        replicaPartitions.put(X, initSet(alpha));

        SpJ2Manager manager = SpJ2InitUtils.initGraph(minNumReplicas, 1, 2, 0.5f, 15, 0, partitions, friendships, replicaPartitions);

        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(A).getNumReplicas() + manager.getPartitionById(B).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(manager.getUserMasterById(alpha), manager.getUserMasterById(beta));

        assertTrue(calculatedNumReplicas == curNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario11() {
        //Scenario 11: alpha has replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has no replica in B, no other friendships exist, alpha exceeds minimum redundancy
        //Result: numReplicas increases by 1

        Scenario scenario11 = getScenario11();
        SpJ2Manager manager = scenario11.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario11.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario11.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario11.alpha, scenario11.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas + 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario12() {
        //Scenario 12: alpha has no replica in B, beta has replica in A, beta has a friend aleph in A, no other friendships exist, beta exceeds minimum redundancy
        //Result: numReplicas remains unchanged

        Scenario scenario12 = getScenario12();
        SpJ2Manager manager = scenario12.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario12.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario12.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario12.alpha, scenario12.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario13() {
        //Scenario 13: alpha has no replica in B, beta has no replica in A, alpha is friends with gamma, no other friendships exist, gamma has replica in B, gamma exceeds minimum redundancy
        //Result: numReplicas decreases by 1

        Scenario scenario13 = getScenario13();
        SpJ2Manager manager = scenario13.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario13.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario13.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario13.alpha, scenario13.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario14() {
        //Scenario 14: alpha has no replica in B, beta has no replica in A, alpha is friends with gamma, gamma is friends with aleph in A, no other friendships exist, gamma has no replica in B
        //Result: numReplicas increases by 1

        Scenario scenario = getScenario14();
        SpJ2Manager manager = scenario.manager;
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario.alpha.getBasePid()).getNumReplicas() + manager.getPartitionById(scenario.beta.getBasePid()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario.alpha, scenario.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas + 1);
    }

    @Test
    public void testFindReplicasToAddToTargetPartition() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser firstUser = manager.getUserMasterById(SpJ2TestUtils.STANDAR_USER_ID_ARRAY[0]);
        Set<Integer> targetPartitionIds = new HashSet<>(manager.getPids());
        targetPartitionIds.remove(firstUser.getBasePid());
        targetPartitionIds.removeAll(firstUser.getReplicaPids());
        Integer targetPartitionId = targetPartitionIds.iterator().next();

        assertTrue(strategy.findReplicasToAddToTargetPartition(firstUser, targetPartitionId).isEmpty());

        targetPartitionIds.remove(targetPartitionId);
        Integer otherTargetPartitionId = targetPartitionIds.iterator().next();
        RepUser oldFriend = SpJ2TestUtils.getUserWithMasterOnPartition(manager, otherTargetPartitionId);
        if (oldFriend.getReplicaPids().contains(targetPartitionId)) {
            manager.addReplica(oldFriend, manager.addPartition());
            manager.removeReplica(oldFriend, targetPartitionId);
        }

        manager.befriend(firstUser, oldFriend);

        RepUser newFriend = SpJ2TestUtils.getUserWithMasterOnPartition(manager, targetPartitionId);
        manager.befriend(firstUser, newFriend);

        Set<Integer> replicasToAdd = strategy.findReplicasToAddToTargetPartition(firstUser, targetPartitionId);
        assertTrue(replicasToAdd.size() == 1);
        assertTrue(replicasToAdd.contains(oldFriend.getId()));
    }

    @Test
    public void testFindReplicasInMovingPartitionToDeleteIsolatedMoverFriendsMeetRedundancyRequirement() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser isolatedUser = null;
        for (Integer userId : SpJ2TestUtils.STANDAR_USER_ID_ARRAY) {
            if (manager.getPartitionById(manager.getUserMasterById(userId).getBasePid()).getNumMasters() == 1) {
                isolatedUser = manager.getUserMasterById(userId);
                break;
            }
        }

        Integer partitionWithSingleMasterId = isolatedUser.getBasePid();
        Set<Integer> nonColocatedUsers = new HashSet<>();
        for (Integer userId : SpJ2TestUtils.STANDAR_USER_ID_ARRAY) {
            RepUser user = manager.getUserMasterById(userId);
            if (!user.getReplicaPids().contains(partitionWithSingleMasterId) && !userId.equals(isolatedUser.getId())) {
                nonColocatedUsers.add(userId);
                boolean isolatedIdIsSmaller = isolatedUser.getId() < userId;
                RepUser smallerUser = isolatedIdIsSmaller ? isolatedUser : user;
                RepUser largerUser = !isolatedIdIsSmaller ? isolatedUser : user;
                manager.befriend(smallerUser, largerUser);
                manager.addReplica(user, partitionWithSingleMasterId);
            }
        }

        Set<Integer> replicasToDelete = strategy.findReplicasInMovingPartitionToDelete(isolatedUser, new HashSet<Integer>());
        assertEquals(replicasToDelete, nonColocatedUsers);
    }

    @Test
    public void testFindReplicasInMovingPartitionToDeleteIsolatedMoverNotAllFriendsMeetRedundancyRequirement() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser isolatedUser = null;
        for (Integer userId : SpJ2TestUtils.STANDAR_USER_ID_ARRAY) {
            if (manager.getPartitionById(manager.getUserMasterById(userId).getBasePid()).getNumMasters() == 1) {
                isolatedUser = manager.getUserMasterById(userId);
                break;
            }
        }

        Integer partitionWithSingleMasterId = isolatedUser.getBasePid();
        //Got a NoSuchElementException on the following line once...
        Integer colocatedFriendId = manager.getPartitionById(partitionWithSingleMasterId).getIdsOfReplicas().iterator().next();

        Set<Integer> nonColocatedUsers = new HashSet<>();
        for (Integer userId : SpJ2TestUtils.STANDAR_USER_ID_ARRAY) {
            RepUser user = manager.getUserMasterById(userId);
            if (!user.getReplicaPids().contains(partitionWithSingleMasterId) && !userId.equals(isolatedUser.getId())) {
                nonColocatedUsers.add(userId);
                boolean isolatedIdIsSmaller = isolatedUser.getId() < userId;
                RepUser smallerUser = isolatedIdIsSmaller ? isolatedUser : user;
                RepUser largerUser = !isolatedIdIsSmaller ? isolatedUser : user;
                manager.befriend(smallerUser, largerUser);
                manager.addReplica(user, partitionWithSingleMasterId);
            }
        }

        manager.befriend(isolatedUser, manager.getUserMasterById(colocatedFriendId));

        Set<Integer> replicasToDelete = strategy.findReplicasInMovingPartitionToDelete(isolatedUser, new HashSet<Integer>());
        assertFalse(replicasToDelete.contains(colocatedFriendId));
        assertEquals(replicasToDelete, nonColocatedUsers);
    }

    @Test
    public void testFindReplicasInMovingPartitionToDeleteIsolatedMoverFriendsMeetRedundancyRequirementWithNewlyAddedReplicas() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser isolatedUser = null;
        for (Integer userId : SpJ2TestUtils.STANDAR_USER_ID_ARRAY) {
            if (manager.getPartitionById(manager.getUserMasterById(userId).getBasePid()).getNumMasters() == 1) {
                isolatedUser = manager.getUserMasterById(userId);
                break;
            }
        }

        Integer partitionWithSingleMasterId = isolatedUser.getBasePid();

        //Got a NoSuchElementException on the following line once
        Integer colocatedFriendId = manager.getPartitionById(partitionWithSingleMasterId).getIdsOfReplicas().iterator().next();
        manager.befriend(isolatedUser, manager.getUserMasterById(colocatedFriendId));

        Set<Integer> nonColocatedUsers = new HashSet<>();
        for (Integer userId : SpJ2TestUtils.STANDAR_USER_ID_ARRAY) {
            RepUser user = manager.getUserMasterById(userId);
            if (!user.getReplicaPids().contains(partitionWithSingleMasterId) && !userId.equals(isolatedUser.getId())) {
                nonColocatedUsers.add(userId);
                boolean isolatedIdIsSmaller = isolatedUser.getId() < userId;
                RepUser smallerUser = isolatedIdIsSmaller ? isolatedUser : user;
                RepUser largerUser = !isolatedIdIsSmaller ? isolatedUser : user;
                manager.befriend(smallerUser, largerUser);
                manager.addReplica(user, partitionWithSingleMasterId);
            }
        }

        Set<Integer> replicasToDelete = strategy.findReplicasInMovingPartitionToDelete(isolatedUser, singleton(colocatedFriendId));
        assertTrue(replicasToDelete.contains(colocatedFriendId));
        nonColocatedUsers.add(colocatedFriendId);
        assertEquals(replicasToDelete, nonColocatedUsers);
    }

    @Test
    public void testFindReplicasInPartitionThatWereOnlyThereForThisUsersSake() {
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        Set<Integer> usersInPartitionWithSingleMaster = new HashSet<>();
        for (Integer userId : SpJ2TestUtils.STANDAR_USER_ID_ARRAY) {
            if (manager.getPartitionById(manager.getUserMasterById(userId).getBasePid()).getNumMasters() == 1) {
                usersInPartitionWithSingleMaster.add(userId);
            }
        }

        RepUser isolatedUser = manager.getUserMasterById(usersInPartitionWithSingleMaster.iterator().next());
        SpJ2Partition partitionWithSingleMaster = manager.getPartitionById(isolatedUser.getBasePid());
        Set<Integer> nonColocatedUsers = new HashSet<>();
        for (Integer userId : SpJ2TestUtils.STANDAR_USER_ID_ARRAY) {
            if (!partitionWithSingleMaster.getIdsOfReplicas().contains(userId) && !userId.equals(isolatedUser.getId())) {
                RepUser nonColocatedFriend = manager.getUserMasterById(userId);
                nonColocatedUsers.add(userId);
                boolean isolatedIdIsSmaller = isolatedUser.getId() < userId;
                RepUser smallerUser = isolatedIdIsSmaller ? isolatedUser : nonColocatedFriend;
                RepUser largerUser = !isolatedIdIsSmaller ? isolatedUser : nonColocatedFriend;
                manager.befriend(smallerUser, largerUser);
                manager.addReplica(nonColocatedFriend, partitionWithSingleMaster.getId());
            }
        }

        Set<Integer> strandedReplicas = strategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(isolatedUser);
        assertEquals(strandedReplicas, nonColocatedUsers);

        //Got a NoSuchElementException on the following line once
        Integer idToUnfriend = nonColocatedUsers.iterator().next();
        nonColocatedUsers.remove(idToUnfriend);
        manager.unfriend(isolatedUser, manager.getUserMasterById(idToUnfriend));
        manager.removeReplica(manager.getUserMasterById(idToUnfriend), isolatedUser.getBasePid());

        strandedReplicas = strategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(isolatedUser);
        assertEquals(strandedReplicas, nonColocatedUsers);
    }

    @Test
    public void testShouldWeDeleteReplicaOfMovingUserInStayingPartitionNoReplica() {
        //Scenario 1: moving user has no replica in staying partition - should result in a false
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser movingUser = manager.getUserMasterById(manager.getUids().iterator().next());
        Integer partitionWithoutThisUser = SpJ2TestUtils.getPartitionsWithNoPresence(manager, movingUser.getId()).iterator().next();
        RepUser stayingUser = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionWithoutThisUser);

        assertFalse(strategy.shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfMovingUserInStayingPartitionReplicaAtMinimum() {
        //Scenario 2: moving user has a replica in staying partition, but doesn't exceed the minimum replication threshold - should result in a false
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser movingUser = manager.getUserMasterById(manager.getUids().iterator().next());
        RepUser stayingUser = SpJ2TestUtils.getUserWithMasterOnPartition(manager, movingUser.getReplicaPids().iterator().next());

        assertFalse(strategy.shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfMovingUserInStayingPartitionReplicaExceedsMinimum() {
        //Scenario 3: moving user has a replica in staying partition and exceeds the minimum replication threshold - should result in a true
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser movingUser = manager.getUserMasterById(manager.getUids().iterator().next());
        Integer partitionInitiallyWithoutThisUser = SpJ2TestUtils.getPartitionsWithNoPresence(manager, movingUser.getId()).iterator().next();
        RepUser stayingUser = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionInitiallyWithoutThisUser);
        manager.addReplica(movingUser, partitionInitiallyWithoutThisUser);

        boolean result = strategy.shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser);
        assertTrue(result);
    }

    @Test
    public void testShouldWeDeleteReplicaOfStayingUserInMovingPartitionNoReplica() {
        //Scenario 1: staying user has no replica in moving partition - should result in a false
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser stayingUser = manager.getUserMasterById(manager.getUids().iterator().next());
        Integer partitionWithoutThisUser = SpJ2TestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        RepUser movingUser = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionWithoutThisUser);

        assertFalse(strategy.shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfStayingUserInMovingPartitionReplicaOtherFriends() {
        //Scenario 2: staying user has a replica in moving partition, and exceeds the minimum replication requirements, but has other friends in that partition - should result in a false
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        SpJ2Partition movingPartition = manager.getPartitionById(SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next());
        RepUser stayingUser = manager.getUserMasterById(movingPartition.getIdsOfReplicas().iterator().next());
        RepUser movingUser = manager.getUserMasterById(movingPartition.getIdsOfMasters().iterator().next());

        Set<Integer> otherUsersOnMovingPartition = new HashSet<>(movingPartition.getIdsOfMasters());
        otherUsersOnMovingPartition.remove(movingUser.getId());
        RepUser otherUserOnMovingPartition = manager.getUserMasterById(otherUsersOnMovingPartition.iterator().next());
        manager.befriend(otherUserOnMovingPartition, stayingUser);

        Integer partitionIdWithoutStayingUser = SpJ2TestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        manager.addReplica(stayingUser, partitionIdWithoutStayingUser);

        assertFalse(strategy.shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfStayingUserInMovingPartitionReplicaNoOtherFriendsAtMinimum() {
        //Scenario 3: staying user has a replica in moving partition, and no other friends in that partition, but doesn't exceed the minimum replication threshold - should result in a false
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser movingUser = manager.getUserMasterById(manager.getUids().iterator().next());
        RepUser stayingUser = SpJ2TestUtils.getUserWithMasterOnPartition(manager, movingUser.getReplicaPids().iterator().next());
        assertFalse(strategy.shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfStayingUserInMovingPartitionReplicaNoOtherFriendsExceedsMinimum() {
        //Scenario 4: staying user has a replica in moving partition, and no other friends in that partition, and exceeds the minimum replication threshold - should result in a true
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser movingUser = manager.getUserMasterById(manager.getUids().iterator().next());
        RepUser stayingUser = SpJ2TestUtils.getUserWithMasterOnPartition(manager, movingUser.getReplicaPids().iterator().next());
        Integer paritionIdWithoutStayingUser = SpJ2TestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        manager.addReplica(stayingUser, paritionIdWithoutStayingUser);
        assertTrue(strategy.shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testCouldWeDeleteReplicaOfStayingUserInMovingPartitionNoReplica() {
        //Scenario 1: staying user has no replica in moving partition - should result in a false
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser stayingUser = manager.getUserMasterById(manager.getUids().iterator().next());
        Integer partitionWithoutThisUser = SpJ2TestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        RepUser movingUser = SpJ2TestUtils.getUserWithMasterOnPartition(manager, partitionWithoutThisUser);

        assertFalse(strategy.couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testCouldWeDeleteReplicaOfStayingUserInMovingPartitionReplicaOtherFriends() {
        //Scenario 2: staying user has a replica in moving partition, but has other friends in that partition - should result in a false
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        //We want to move from a partition that has two masters and at least one replica
        SpJ2Partition movingPartition = null;
        for(Integer pid : SpJ2TestUtils.getPartitionIdsWithNMasters(manager, 2)) {
            SpJ2Partition tempPartition = manager.getPartitionById(pid);
            if(!tempPartition.getIdsOfReplicas().isEmpty()) {
                movingPartition = tempPartition;
            }
        }

        RepUser stayingUser = manager.getUserMasterById(movingPartition.getIdsOfReplicas().iterator().next());
        RepUser movingUser = manager.getUserMasterById(movingPartition.getIdsOfMasters().iterator().next());

        Set<Integer> otherUsersOnMovingPartition = new HashSet<>(movingPartition.getIdsOfMasters());
        otherUsersOnMovingPartition.remove(movingUser.getId());
        RepUser otherUserOnMovingPartition = manager.getUserMasterById(otherUsersOnMovingPartition.iterator().next());
        manager.befriend(otherUserOnMovingPartition, stayingUser);

        Integer partitionIdWithoutStayingUser = SpJ2TestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        manager.addReplica(stayingUser, partitionIdWithoutStayingUser);

        assertFalse(strategy.couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testCouldWeDeleteReplicaOfStayingUserInMovingPartitionNoOtherFriends() {
        //Scenario 3: staying user has a replica in moving partition, and no other friends in that partition - should result in a true
        SpJ2Manager manager = SpJ2TestUtils.getStandardManager();
        SpJ2BefriendingStrategy strategy = new SpJ2BefriendingStrategy(manager);

        RepUser stayingUser = manager.getUserMasterById(manager.getUids().iterator().next());
        RepUser movingUser = SpJ2TestUtils.getUserWithMasterOnPartition(manager, stayingUser.getReplicaPids().iterator().next());

        assertTrue(strategy.couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }
}