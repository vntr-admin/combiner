package io.vntr.spar;

import java.util.*;

import org.junit.Test;

import static io.vntr.TestUtils.initSet;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.LARGE_TO_SMALL;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.NO_CHANGE;
import static io.vntr.spar.BEFRIEND_REBALANCE_STRATEGY.SMALL_TO_LARGE;
import static org.junit.Assert.*;

public class SparBefriendingStrategyTest {
    private static class Scenario {
        public final SparManager manager;
        public final SparUser alpha;
        public final SparUser beta;
        public final SparUser aleph;
        public final SparUser gamma;

        public Scenario(SparManager manager, SparUser alpha, SparUser beta, SparUser aleph, SparUser gamma) {
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
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = manager.getAllPartitionIds().iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long newReplicaLocation = SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        //ensure that A and B exceed redundancy requirements
        Set<Long> partitionsWithoutAlphaOrBetaMasters = new HashSet<Long>(manager.getAllPartitionIds());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Long extraAlphaReplicaLocation = null;
        Long extraBetaReplicaLocation = null;
        for (Long partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPartitionIds().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPartitionIds().contains(partitionId)) {
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
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = manager.getAllPartitionIds().iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = alpha.getReplicaPartitionIds().iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long newReplicaLocation = SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        //ensure that A and B exceed redundancy requirements
        Set<Long> partitionsWithoutAlphaOrBetaMasters = new HashSet<Long>(manager.getAllPartitionIds());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Long extraAlphaReplicaLocation = null;
        Long extraBetaReplicaLocation = null;
        for (Long partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPartitionIds().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPartitionIds().contains(partitionId)) {
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
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = manager.getAllPartitionIds().iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = alpha.getReplicaPartitionIds().iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long newReplicaLocation = SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        Set<Long> locationsWhereWeCouldReplicateBeta = new HashSet<Long>(SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()));
        locationsWhereWeCouldReplicateBeta.remove(partitionAId);
        manager.addReplica(beta, locationsWhereWeCouldReplicateBeta.iterator().next());

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has replica in B, beta has replica in A, alpha and beta have no friends, alpha and beta both exceed minimum redundancy
     */
    private static Scenario getScenario4() {
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = manager.getAllPartitionIds().iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = alpha.getReplicaPartitionIds().iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long betaReplicaToNix = beta.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        Set<Long> partitionsWithoutAlphaOrBetaMasters = new HashSet<Long>(manager.getAllPartitionIds());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Long extraAlphaReplicaLocation = null;
        Long extraBetaReplicaLocation = null;
        for (Long partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPartitionIds().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPartitionIds().contains(partitionId)) {
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
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = manager.getAllPartitionIds().iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = alpha.getReplicaPartitionIds().iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long betaReplicaToNix = beta.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        manager.addReplica(beta, SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next());

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has replica in B, beta has replica in A, alpha and beta have no friends, alpha exceeds minimum redundancy but beta does not
     */
    private static Scenario getScenario6() {
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = manager.getAllPartitionIds().iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = alpha.getReplicaPartitionIds().iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long betaReplicaToNix = beta.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        manager.addReplica(alpha, SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next());

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has replica in B, beta has replica in A, alpha and beta have no friends, neither alpha nor beta exceeds minimum redundancy
     */
    private static Scenario getScenario7() {
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = manager.getAllPartitionIds().iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = alpha.getReplicaPartitionIds().iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long betaReplicaToNix = beta.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        return new Scenario(manager, alpha, beta, null, null);
    }

    /**
     * alpha has no replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has a replica in B, no other friendships exist, both alpha and beta exceed minimum redundancy
     */
    private static Scenario getScenario8() {
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long newReplicaLocation = SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        Set<Long> partitionsWithoutAlphaOrBetaMasters = new HashSet<Long>(manager.getAllPartitionIds());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Long extraAlphaReplicaLocation = null;
        Long extraBetaReplicaLocation = null;
        for (Long partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPartitionIds().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPartitionIds().contains(partitionId)) {
                extraBetaReplicaLocation = partitionId;
            }
        }
        assertNotNull(extraAlphaReplicaLocation);
        assertNotNull(extraBetaReplicaLocation);
        manager.addReplica(alpha, extraAlphaReplicaLocation);
        manager.addReplica(beta, extraBetaReplicaLocation);

        Set<Long> otherUsersInA = new HashSet<Long>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());
        SparUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());
        manager.befriend(alpha, aleph);

        //ensure aleph has a replica in B
        if (!aleph.getReplicaPartitionIds().contains(partitionBId)) {
            Long partitionForAlephToLeave = aleph.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(aleph, partitionForAlephToLeave);
            manager.addReplica(aleph, partitionBId);
        }

        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has no replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has no replica in B, no other friendships exist, both alpha and beta exceed minimum redundancy
     */
    private static Scenario getScenario9() {
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long newReplicaLocation = SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        Set<Long> partitionsWithoutAlphaOrBetaMasters = new HashSet<Long>(manager.getAllPartitionIds());
        partitionsWithoutAlphaOrBetaMasters.remove(partitionAId);
        partitionsWithoutAlphaOrBetaMasters.remove(partitionBId);
        Long extraAlphaReplicaLocation = null;
        Long extraBetaReplicaLocation = null;
        for (Long partitionId : partitionsWithoutAlphaOrBetaMasters) {
            if (!alpha.getReplicaPartitionIds().contains(partitionId)) {
                extraAlphaReplicaLocation = partitionId;
            }
            if (!beta.getReplicaPartitionIds().contains(partitionId)) {
                extraBetaReplicaLocation = partitionId;
            }
        }
        assertNotNull(extraAlphaReplicaLocation);
        assertNotNull(extraBetaReplicaLocation);
        manager.addReplica(alpha, extraAlphaReplicaLocation);
        manager.addReplica(beta, extraBetaReplicaLocation);

        Set<Long> otherUsersInA = new HashSet<Long>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());
        SparUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());
        manager.befriend(alpha, aleph);

        //ensure aleph doesn't have a replica in B
        if (aleph.getReplicaPartitionIds().contains(partitionBId)) {
            Long newPartitionId = SparTestUtils.getPartitionsWithNoPresence(manager, aleph.getId()).iterator().next();
            manager.addReplica(aleph, newPartitionId);
            manager.removeReplica(aleph, partitionBId);
        }

        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has a replica in B, no other friendships exist, alpha exceeds minimum redundancy
     */
    private static Scenario getScenario10() {
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = alpha.getReplicaPartitionIds().iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        Set<Long> otherUsersInA = new HashSet<Long>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());

        SparUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());
        manager.befriend(alpha, aleph);

        if (beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long newReplicaLocation = SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        //ensure aleph has a replica in B
        if (!aleph.getReplicaPartitionIds().contains(partitionBId)) {
            Long partitionForAlephToLeave = aleph.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(aleph, partitionForAlephToLeave);
            manager.addReplica(aleph, partitionBId);
        }

        Long extraPartitionForAlphaReplica = SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        manager.addReplica(alpha, extraPartitionForAlphaReplica);

        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has no replica in B, no other friendships exist, alpha exceeds minimum redundancy
     */
    private static Scenario getScenario11() {
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = alpha.getReplicaPartitionIds().iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        Set<Long> otherUsersInA = new HashSet<Long>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());

        SparUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());
        manager.befriend(alpha, aleph);

        if (beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long newReplicaLocation = SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
            manager.removeReplica(beta, partitionAId);
            manager.addReplica(beta, newReplicaLocation);
        }

        //ensure aleph doesn't have a replica in B
        if (aleph.getReplicaPartitionIds().contains(partitionBId)) {
            Long newPartitionId = SparTestUtils.getPartitionsWithNoPresence(manager, aleph.getId()).iterator().next();
            manager.addReplica(aleph, newPartitionId);
            manager.removeReplica(aleph, partitionBId);
        }
        Long extraPartitionForAlphaReplica = SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        manager.addReplica(alpha, extraPartitionForAlphaReplica);
        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has no replica in B, beta has replica in A, beta has a friend aleph in A, no other friendships exist, beta exceeds minimum redundancy
     */
    private static Scenario getScenario12() {
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Set<Long> otherUsersInA = new HashSet<Long>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());

        SparUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());

        Long partitionBId = SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long betaReplicaToNix = beta.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(beta, betaReplicaToNix);
            manager.addReplica(beta, partitionAId);
        }

        if (!aleph.getReplicaPartitionIds().contains(partitionBId)) {
            Long alephReplicaToNix = aleph.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(aleph, alephReplicaToNix);
            manager.addReplica(aleph, partitionBId);
        }

        manager.addReplica(beta, SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next());

        manager.befriend(beta, aleph);

        return new Scenario(manager, alpha, beta, aleph, null);
    }

    /**
     * alpha has no replica in B, beta has no replica in A, alpha is friends with gamma, no other friendships exist, gamma has replica in B, gamma exceeds minimum redundancy
     */
    private static Scenario getScenario13() {
        //Scenario 13: alpha has no replica in B, beta has no replica in A, alpha is friends with gamma, no other friendships exist, gamma has replica in B, gamma exceeds minimum redundancy
        //Result: numReplicas decreases by 1

        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = manager.getAllPartitionIds().iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Long partitionBId = SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        Set<Long> otherPartitions = new HashSet<Long>(manager.getAllPartitionIds());
        otherPartitions.remove(partitionAId);
        otherPartitions.remove(partitionBId);
        Long partitionCId = otherPartitions.iterator().next();
        SparUser gamma = SparTestUtils.getUserWithMasterOnPartition(manager, partitionCId);

        //Effectively, gamma needs to have replicas on precisely partitionA and partitionB, and nowhere else, so we just remove all existing ones and add those two manually, plus one other one for redundancy
        for (Long gammaReplicaId : new HashSet<Long>(gamma.getReplicaPartitionIds())) {
            manager.removeReplica(gamma, gammaReplicaId);
        }
        manager.addReplica(gamma, partitionAId);
        manager.addReplica(gamma, partitionBId);
        manager.addReplica(gamma, SparTestUtils.getPartitionsWithNoPresence(manager, gamma.getId()).iterator().next());

        if (!alpha.getReplicaPartitionIds().contains(partitionCId)) {
            Long alphaReplicaToNix = alpha.getReplicaPartitionIds().iterator().next();
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
        SparManager manager = SparTestUtils.getStandardManager();

        Long partitionAId = SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);

        Set<Long> otherUsersInA = new HashSet<Long>(manager.getPartitionById(partitionAId).getIdsOfMasters());
        otherUsersInA.remove(alpha.getId());

        SparUser aleph = manager.getUserMasterById(otherUsersInA.iterator().next());

        Long partitionBId = SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        Set<Long> otherPartitions = new HashSet<Long>(manager.getAllPartitionIds());
        otherPartitions.remove(partitionAId);
        otherPartitions.remove(partitionBId);
        Long partitionCId = otherPartitions.iterator().next();
        SparUser gamma = SparTestUtils.getUserWithMasterOnPartition(manager, partitionCId);

        //Since gamma and alpha are friends, they need replicas on each other's partitions.
        //Same with gamma and aleph, though the above will already insure gamma has a replica on aleph's partition
        if (!gamma.getReplicaPartitionIds().contains(partitionAId)) {
            Long partitionToDitch = gamma.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(gamma, partitionToDitch);
            manager.addReplica(gamma, partitionAId);
        }
        if (!alpha.getReplicaPartitionIds().contains(partitionCId)) {
            Long partitionToDitch = alpha.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(alpha, partitionToDitch);
            manager.addReplica(alpha, partitionCId);
        }
        if (!aleph.getReplicaPartitionIds().contains(partitionCId)) {
            Long partitionToDitch = aleph.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(aleph, partitionToDitch);
            manager.addReplica(aleph, partitionCId);
        }

        //gamma should not have a replica on B
        if (gamma.getReplicaPartitionIds().contains(partitionBId)) {
            Long partitionToAddGamma = SparTestUtils.getPartitionsWithNoPresence(manager, gamma.getId()).iterator().next();
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
            assertEquals(SparBefriendingStrategy.determineStrategy(4, 4, 4, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
            assertEquals(SparBefriendingStrategy.determineStrategy(4, 5, 4, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
            assertEquals(SparBefriendingStrategy.determineStrategy(4, 4, 5, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
            assertEquals(SparBefriendingStrategy.determineStrategy(4, 5, 5, (int) (Math.random() * 20), (int) (Math.random() * 20)), NO_CHANGE);
        }

        assertEquals(SparBefriendingStrategy.determineStrategy(5, 4, 5, 2, 3), LARGE_TO_SMALL);
        assertEquals(SparBefriendingStrategy.determineStrategy(5, 4, 5, 5, 5), LARGE_TO_SMALL);
        assertEquals(SparBefriendingStrategy.determineStrategy(5, 4, 5, 3, 3), NO_CHANGE);

        assertEquals(SparBefriendingStrategy.determineStrategy(5, 4, 4, 2, 3), LARGE_TO_SMALL);
        assertEquals(SparBefriendingStrategy.determineStrategy(5, 4, 4, 3, 3), NO_CHANGE);

        assertEquals(SparBefriendingStrategy.determineStrategy(5, 5, 4, 3, 2), SMALL_TO_LARGE);
        assertEquals(SparBefriendingStrategy.determineStrategy(5, 5, 4, 5, 5), SMALL_TO_LARGE);
        assertEquals(SparBefriendingStrategy.determineStrategy(5, 5, 4, 3, 3), NO_CHANGE);
    }

    @Test
    public void testCalcNumReplicasNoMovementNoCrossReplicas() {
        //Suppose that user alpha is from partition A with 1 master, while user beta is from a partition B with 2 masters
        //Scenario 1: no replicas of alpha on B, no replicas of beta on A
        //Result: should be 2 replicas

        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        Long partitionAId = SparTestUtils.getPartitionIdsWithNMasters(manager, 1).iterator().next();
        Long partitionBId = SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SparPartition partitionA = manager.getPartitionById(partitionAId);
        SparPartition partitionB = manager.getPartitionById(partitionBId);
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (alpha.getReplicaPartitionIds().contains(partitionBId)) {
            Long partitionWithoutAlpha = SparTestUtils.getPartitionsWithNoPresence(manager, alpha.getId()).iterator().next();
            manager.addReplica(alpha, partitionWithoutAlpha);
            manager.removeReplica(alpha, partitionBId);
        }

        if (beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long partitionWithoutBeta = SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
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
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        Long partitionAId = SparTestUtils.getPartitionIdsWithNMasters(manager, 1).iterator().next();
        Long partitionBId = SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SparPartition partitionA = manager.getPartitionById(partitionAId);
        SparPartition partitionB = manager.getPartitionById(partitionBId);
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!alpha.getReplicaPartitionIds().contains(partitionBId)) {
            Long partitionFromWhichToRemoveAlpha = alpha.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(alpha, partitionFromWhichToRemoveAlpha);
            manager.addReplica(alpha, partitionBId);
        }

        if (beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long partitionWithoutBeta = SparTestUtils.getPartitionsWithNoPresence(manager, beta.getId()).iterator().next();
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
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        Long partitionAId = SparTestUtils.getPartitionIdsWithNMasters(manager, 1).iterator().next();
        Long partitionBId = SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next();
        SparPartition partitionA = manager.getPartitionById(partitionAId);
        SparPartition partitionB = manager.getPartitionById(partitionBId);
        SparUser alpha = SparTestUtils.getUserWithMasterOnPartition(manager, partitionAId);
        SparUser beta = SparTestUtils.getUserWithMasterOnPartition(manager, partitionBId);

        if (!alpha.getReplicaPartitionIds().contains(partitionBId)) {
            Long partitionFromWhichToRemoveAlpha = alpha.getReplicaPartitionIds().iterator().next();
            manager.removeReplica(alpha, partitionFromWhichToRemoveAlpha);
            manager.addReplica(alpha, partitionBId);
        }

        if (!beta.getReplicaPartitionIds().contains(partitionAId)) {
            Long partitionFromWhichToRemoveBeta = beta.getReplicaPartitionIds().iterator().next();
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
        SparManager manager = scenario1.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario1.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario1.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario1.alpha, scenario1.beta);
        assertTrue(curNumReplicas == calculatedNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario2() {
        //Scenario 2: alpha has replica in B, beta has no replica in A, alpha and beta have no friends, alpha and beta exceed minimum redundancy
        //Result: numReplicas decreases by 1

        Scenario scenario2 = getScenario2();
        SparManager manager = scenario2.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario2.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario2.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario2.alpha, scenario2.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario3() {
        //Scenario 3: alpha has replica in B, beta has no replica in A, alpha and beta have no friends, alpha does not exceed minimum redundancy, but beta does
        //Result: numReplicas unchanged

        Scenario scenario3 = getScenario3();
        SparManager manager = scenario3.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario3.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario3.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario3.alpha, scenario3.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario4() {
        //Scenario 4: alpha has replica in B, beta has replica in A, alpha and beta have no friends, alpha and beta both exceed minimum redundancy
        //Result: numReplicas decreases by 2

        Scenario scenario4 = getScenario4();
        SparManager manager = scenario4.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario4.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario4.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario4.alpha, scenario4.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 2);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario5() {
        //Scenario 5: alpha has replica in B, beta has replica in A, alpha and beta have no friends, beta exceeds minimum redundancy but alpha does not
        //Result: numReplicas decreases by 1

        Scenario scenario5 = getScenario5();
        SparManager manager = scenario5.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario5.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario5.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario5.alpha, scenario5.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario6() {
        //Scenario 6: alpha has replica in B, beta has replica in A, alpha and beta have no friends, alpha exceeds minimum redundancy but beta does not
        //Result: numReplicas decreases by 1

        Scenario scenario6 = getScenario6();
        SparManager manager = scenario6.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario6.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario6.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario6.alpha, scenario6.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario7() {
        //Scenario 7: alpha has replica in B, beta has replica in A, alpha and beta have no friends, neither alpha nor beta exceeds minimum redundancy
        //Result: numReplicas remains unchanged

        Scenario scenario7 = getScenario7();
        SparManager manager = scenario7.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario7.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario7.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario7.alpha, scenario7.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario8() {
        //Scenario 8: alpha has no replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has a replica in B, no other friendships exist, both alpha and beta exceed minimum redundancy
        //Result: numReplicas increases by 1

        Scenario scenario8 = getScenario8();
        SparManager manager = scenario8.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario8.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario8.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario8.alpha, scenario8.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas + 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario9() {
        //Scenario 9: alpha has no replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has no replica in B, no other friendships exist, both alpha and beta exceed minimum redundancy
        //Result: numReplicas increases by 1

        Scenario scenario9 = getScenario9();
        SparManager manager = scenario9.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario9.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario9.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario9.alpha, scenario9.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas + 2);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario10() {
        //Scenario 10: alpha has replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has a replica in B, no other friendships exist, alpha exceeds minimum redundancy
        //Result: numReplicas remains unchanged

        Long A = 1L;
        Long B = 2L;
        Long X = 99L;
        Long alpha = 1L;
        Long beta  = 2L;
        Long aleph = 3L;
        Long xi = 112L;

        int minNumReplicas = 1;
        Map<Long, Set<Long>> partitions = new HashMap<Long, Set<Long>>();
        partitions.put(A, initSet(alpha, aleph));
        partitions.put(B, initSet( beta));
        partitions.put(X, initSet(xi));

        Map<Long, Set<Long>> friendships = new HashMap<Long, Set<Long>>();
        friendships.put(alpha, initSet(aleph));
        friendships.put(beta,  Collections.<Long>emptySet());
        friendships.put(aleph, Collections.<Long>emptySet());
        friendships.put(xi,    Collections.<Long>emptySet());

        Map<Long, Set<Long>> replicaPartitions = new HashMap<Long, Set<Long>>();
        replicaPartitions.put(A, initSet(xi));
        replicaPartitions.put(B, initSet(alpha, aleph));
        replicaPartitions.put(X, initSet(alpha));

        SparManager manager = SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicaPartitions);

        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(A).getNumReplicas() + manager.getPartitionById(B).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(manager.getUserMasterById(alpha), manager.getUserMasterById(beta));

        assertTrue(calculatedNumReplicas == curNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario11() {
        //Scenario 11: alpha has replica in B, beta has no replica in A, alpha has friend aleph in A, aleph has no replica in B, no other friendships exist, alpha exceeds minimum redundancy
        //Result: numReplicas increases by 1

        Scenario scenario11 = getScenario11();
        SparManager manager = scenario11.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario11.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario11.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario11.alpha, scenario11.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas + 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario12() {
        //Scenario 12: alpha has no replica in B, beta has replica in A, beta has a friend aleph in A, no other friendships exist, beta exceeds minimum redundancy
        //Result: numReplicas remains unchanged

        Scenario scenario12 = getScenario12();
        SparManager manager = scenario12.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario12.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario12.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario12.alpha, scenario12.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario13() {
        //Scenario 13: alpha has no replica in B, beta has no replica in A, alpha is friends with gamma, no other friendships exist, gamma has replica in B, gamma exceeds minimum redundancy
        //Result: numReplicas decreases by 1

        Scenario scenario13 = getScenario13();
        SparManager manager = scenario13.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario13.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario13.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario13.alpha, scenario13.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas - 1);
    }

    @Test
    public void testCalcNumReplicasOneMovesToOtherScenario14() {
        //Scenario 14: alpha has no replica in B, beta has no replica in A, alpha is friends with gamma, gamma is friends with aleph in A, no other friendships exist, gamma has no replica in B
        //Result: numReplicas increases by 1

        Scenario scenario = getScenario14();
        SparManager manager = scenario.manager;
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        int curNumReplicas = manager.getPartitionById(scenario.alpha.getMasterPartitionId()).getNumReplicas() + manager.getPartitionById(scenario.beta.getMasterPartitionId()).getNumReplicas();
        int calculatedNumReplicas = strategy.calcNumReplicasMove(scenario.alpha, scenario.beta);

        assertTrue(calculatedNumReplicas == curNumReplicas + 1);
    }

    @Test
    public void testFindReplicasToAddToTargetPartition() {
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser firstUser = manager.getUserMasterById(SparTestUtils.STANDAR_USER_ID_ARRAY[0]);
        Set<Long> targetPartitionIds = new HashSet<Long>(manager.getAllPartitionIds());
        targetPartitionIds.remove(firstUser.getMasterPartitionId());
        targetPartitionIds.removeAll(firstUser.getReplicaPartitionIds());
        Long targetPartitionId = targetPartitionIds.iterator().next();

        assertTrue(strategy.findReplicasToAddToTargetPartition(firstUser, targetPartitionId).isEmpty());

        targetPartitionIds.remove(targetPartitionId);
        Long otherTargetPartitionId = targetPartitionIds.iterator().next();
        SparUser oldFriend = SparTestUtils.getUserWithMasterOnPartition(manager, otherTargetPartitionId);
        if (oldFriend.getReplicaPartitionIds().contains(targetPartitionId)) {
            manager.addReplica(oldFriend, manager.addPartition());
            manager.removeReplica(oldFriend, targetPartitionId);
        }

        manager.befriend(firstUser, oldFriend);

        SparUser newFriend = SparTestUtils.getUserWithMasterOnPartition(manager, targetPartitionId);
        manager.befriend(firstUser, newFriend);

        Set<Long> replicasToAdd = strategy.findReplicasToAddToTargetPartition(firstUser, targetPartitionId);
        assertTrue(replicasToAdd.size() == 1);
        assertTrue(replicasToAdd.contains(oldFriend.getId()));
    }

    @Test
    public void testFindReplicasInMovingPartitionToDeleteIsolatedMoverFriendsMeetRedundancyRequirement() {
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser isolatedUser = null;
        for (Long userId : SparTestUtils.STANDAR_USER_ID_ARRAY) {
            if (manager.getPartitionById(manager.getUserMasterById(userId).getMasterPartitionId()).getNumMasters() == 1) {
                isolatedUser = manager.getUserMasterById(userId);
                break;
            }
        }

        Long partitionWithSingleMasterId = isolatedUser.getMasterPartitionId();
        Set<Long> nonColocatedUsers = new HashSet<Long>();
        for (Long userId : SparTestUtils.STANDAR_USER_ID_ARRAY) {
            SparUser user = manager.getUserMasterById(userId);
            if (!user.getReplicaPartitionIds().contains(partitionWithSingleMasterId) && !userId.equals(isolatedUser.getId())) {
                nonColocatedUsers.add(userId);
                boolean isolatedIdIsSmaller = isolatedUser.getId().longValue() < userId.longValue();
                SparUser smallerUser = isolatedIdIsSmaller ? isolatedUser : user;
                SparUser largerUser = !isolatedIdIsSmaller ? isolatedUser : user;
                manager.befriend(smallerUser, largerUser);
                manager.addReplica(user, partitionWithSingleMasterId);
            }
        }

        Set<Long> replicasToDelete = strategy.findReplicasInMovingPartitionToDelete(isolatedUser, new HashSet<Long>());
        assertEquals(replicasToDelete, nonColocatedUsers);
    }

    @Test
    public void testFindReplicasInMovingPartitionToDeleteIsolatedMoverNotAllFriendsMeetRedundancyRequirement() {
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser isolatedUser = null;
        for (Long userId : SparTestUtils.STANDAR_USER_ID_ARRAY) {
            if (manager.getPartitionById(manager.getUserMasterById(userId).getMasterPartitionId()).getNumMasters() == 1) {
                isolatedUser = manager.getUserMasterById(userId);
                break;
            }
        }

        Long partitionWithSingleMasterId = isolatedUser.getMasterPartitionId();
        //Got a NoSuchElementException on the following line once...
        Long colocatedFriendId = manager.getPartitionById(partitionWithSingleMasterId).getIdsOfReplicas().iterator().next();

        Set<Long> nonColocatedUsers = new HashSet<Long>();
        for (Long userId : SparTestUtils.STANDAR_USER_ID_ARRAY) {
            SparUser user = manager.getUserMasterById(userId);
            if (!user.getReplicaPartitionIds().contains(partitionWithSingleMasterId) && !userId.equals(isolatedUser.getId())) {
                nonColocatedUsers.add(userId);
                boolean isolatedIdIsSmaller = isolatedUser.getId().longValue() < userId.longValue();
                SparUser smallerUser = isolatedIdIsSmaller ? isolatedUser : user;
                SparUser largerUser = !isolatedIdIsSmaller ? isolatedUser : user;
                manager.befriend(smallerUser, largerUser);
                manager.addReplica(user, partitionWithSingleMasterId);
            }
        }

        manager.befriend(isolatedUser, manager.getUserMasterById(colocatedFriendId));

        Set<Long> replicasToDelete = strategy.findReplicasInMovingPartitionToDelete(isolatedUser, new HashSet<Long>());
        assertFalse(replicasToDelete.contains(colocatedFriendId));
        assertEquals(replicasToDelete, nonColocatedUsers);
    }

    @Test
    public void testFindReplicasInMovingPartitionToDeleteIsolatedMoverFriendsMeetRedundancyRequirementWithNewlyAddedReplicas() {
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser isolatedUser = null;
        for (Long userId : SparTestUtils.STANDAR_USER_ID_ARRAY) {
            if (manager.getPartitionById(manager.getUserMasterById(userId).getMasterPartitionId()).getNumMasters() == 1) {
                isolatedUser = manager.getUserMasterById(userId);
                break;
            }
        }

        Long partitionWithSingleMasterId = isolatedUser.getMasterPartitionId();

        //Got a NoSuchElementException on the following line once
        Long colocatedFriendId = manager.getPartitionById(partitionWithSingleMasterId).getIdsOfReplicas().iterator().next();
        manager.befriend(isolatedUser, manager.getUserMasterById(colocatedFriendId));

        Set<Long> nonColocatedUsers = new HashSet<Long>();
        for (Long userId : SparTestUtils.STANDAR_USER_ID_ARRAY) {
            SparUser user = manager.getUserMasterById(userId);
            if (!user.getReplicaPartitionIds().contains(partitionWithSingleMasterId) && !userId.equals(isolatedUser.getId())) {
                nonColocatedUsers.add(userId);
                boolean isolatedIdIsSmaller = isolatedUser.getId().longValue() < userId.longValue();
                SparUser smallerUser = isolatedIdIsSmaller ? isolatedUser : user;
                SparUser largerUser = !isolatedIdIsSmaller ? isolatedUser : user;
                manager.befriend(smallerUser, largerUser);
                manager.addReplica(user, partitionWithSingleMasterId);
            }
        }

        Set<Long> replicasToDelete = strategy.findReplicasInMovingPartitionToDelete(isolatedUser, new HashSet<Long>(Arrays.asList(colocatedFriendId)));
        assertTrue(replicasToDelete.contains(colocatedFriendId));
        nonColocatedUsers.add(colocatedFriendId);
        assertEquals(replicasToDelete, nonColocatedUsers);
    }

    @Test
    public void testFindReplicasInPartitionThatWereOnlyThereForThisUsersSake() {
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        Set<Long> usersInPartitionWithSingleMaster = new HashSet<Long>();
        for (Long userId : SparTestUtils.STANDAR_USER_ID_ARRAY) {
            if (manager.getPartitionById(manager.getUserMasterById(userId).getMasterPartitionId()).getNumMasters() == 1) {
                usersInPartitionWithSingleMaster.add(userId);
            }
        }

        SparUser isolatedUser = manager.getUserMasterById(usersInPartitionWithSingleMaster.iterator().next());
        SparPartition partitionWithSingleMaster = manager.getPartitionById(isolatedUser.getMasterPartitionId());
        Set<Long> nonColocatedUsers = new HashSet<Long>();
        for (Long userId : SparTestUtils.STANDAR_USER_ID_ARRAY) {
            if (!partitionWithSingleMaster.getIdsOfReplicas().contains(userId) && !userId.equals(isolatedUser.getId())) {
                SparUser nonColocatedFriend = manager.getUserMasterById(userId);
                nonColocatedUsers.add(userId);
                boolean isolatedIdIsSmaller = isolatedUser.getId().longValue() < userId.longValue();
                SparUser smallerUser = isolatedIdIsSmaller ? isolatedUser : nonColocatedFriend;
                SparUser largerUser = !isolatedIdIsSmaller ? isolatedUser : nonColocatedFriend;
                manager.befriend(smallerUser, largerUser);
                manager.addReplica(nonColocatedFriend, partitionWithSingleMaster.getId());
            }
        }

        Set<Long> strandedReplicas = strategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(isolatedUser);
        assertEquals(strandedReplicas, nonColocatedUsers);

        //Got a NoSuchElementException on the following line once
        Long idToUnfriend = nonColocatedUsers.iterator().next();
        nonColocatedUsers.remove(idToUnfriend);
        manager.unfriend(isolatedUser, manager.getUserMasterById(idToUnfriend));
        manager.removeReplica(manager.getUserMasterById(idToUnfriend), isolatedUser.getMasterPartitionId());

        strandedReplicas = strategy.findReplicasInPartitionThatWereOnlyThereForThisUsersSake(isolatedUser);
        assertEquals(strandedReplicas, nonColocatedUsers);
    }

    @Test
    public void testShouldWeDeleteReplicaOfMovingUserInStayingPartitionNoReplica() {
        //Scenario 1: moving user has no replica in staying partition - should result in a false
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser movingUser = manager.getUserMasterById(manager.getAllUserIds().iterator().next());
        Long partitionWithoutThisUser = SparTestUtils.getPartitionsWithNoPresence(manager, movingUser.getId()).iterator().next();
        SparUser stayingUser = SparTestUtils.getUserWithMasterOnPartition(manager, partitionWithoutThisUser);

        assertFalse(strategy.shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfMovingUserInStayingPartitionReplicaAtMinimum() {
        //Scenario 2: moving user has a replica in staying partition, but doesn't exceed the minimum replication threshold - should result in a false
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser movingUser = manager.getUserMasterById(manager.getAllUserIds().iterator().next());
        SparUser stayingUser = SparTestUtils.getUserWithMasterOnPartition(manager, movingUser.getReplicaPartitionIds().iterator().next());

        assertFalse(strategy.shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfMovingUserInStayingPartitionReplicaExceedsMinimum() {
        //Scenario 3: moving user has a replica in staying partition and exceeds the minimum replication threshold - should result in a true
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser movingUser = manager.getUserMasterById(manager.getAllUserIds().iterator().next());
        Long partitionInitiallyWithoutThisUser = SparTestUtils.getPartitionsWithNoPresence(manager, movingUser.getId()).iterator().next();
        SparUser stayingUser = SparTestUtils.getUserWithMasterOnPartition(manager, partitionInitiallyWithoutThisUser);
        manager.addReplica(movingUser, partitionInitiallyWithoutThisUser);

        boolean result = strategy.shouldWeDeleteReplicaOfMovingUserInStayingPartition(movingUser, stayingUser);
        assertTrue(result);
    }

    @Test
    public void testShouldWeDeleteReplicaOfStayingUserInMovingPartitionNoReplica() {
        //Scenario 1: staying user has no replica in moving partition - should result in a false
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser stayingUser = manager.getUserMasterById(manager.getAllUserIds().iterator().next());
        Long partitionWithoutThisUser = SparTestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        SparUser movingUser = SparTestUtils.getUserWithMasterOnPartition(manager, partitionWithoutThisUser);

        assertFalse(strategy.shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfStayingUserInMovingPartitionReplicaOtherFriends() {
        //Scenario 2: staying user has a replica in moving partition, and exceeds the minimum replication requirements, but has other friends in that partition - should result in a false
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparPartition movingPartition = manager.getPartitionById(SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next());
        SparUser stayingUser = manager.getUserMasterById(movingPartition.getIdsOfReplicas().iterator().next());
        SparUser movingUser = manager.getUserMasterById(movingPartition.getIdsOfMasters().iterator().next());

        Set<Long> otherUsersOnMovingPartition = new HashSet<Long>(movingPartition.getIdsOfMasters());
        otherUsersOnMovingPartition.remove(movingUser.getId());
        SparUser otherUserOnMovingPartition = manager.getUserMasterById(otherUsersOnMovingPartition.iterator().next());
        manager.befriend(otherUserOnMovingPartition, stayingUser);

        Long partitionIdWithoutStayingUser = SparTestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        manager.addReplica(stayingUser, partitionIdWithoutStayingUser);

        assertFalse(strategy.shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfStayingUserInMovingPartitionReplicaNoOtherFriendsAtMinimum() {
        //Scenario 3: staying user has a replica in moving partition, and no other friends in that partition, but doesn't exceed the minimum replication threshold - should result in a false
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser movingUser = manager.getUserMasterById(manager.getAllUserIds().iterator().next());
        SparUser stayingUser = SparTestUtils.getUserWithMasterOnPartition(manager, movingUser.getReplicaPartitionIds().iterator().next());
        assertFalse(strategy.shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testShouldWeDeleteReplicaOfStayingUserInMovingPartitionReplicaNoOtherFriendsExceedsMinimum() {
        //Scenario 4: staying user has a replica in moving partition, and no other friends in that partition, and exceeds the minimum replication threshold - should result in a true
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser movingUser = manager.getUserMasterById(manager.getAllUserIds().iterator().next());
        SparUser stayingUser = SparTestUtils.getUserWithMasterOnPartition(manager, movingUser.getReplicaPartitionIds().iterator().next());
        Long paritionIdWithoutStayingUser = SparTestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        manager.addReplica(stayingUser, paritionIdWithoutStayingUser);
        assertTrue(strategy.shouldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testCouldWeDeleteReplicaOfStayingUserInMovingPartitionNoReplica() {
        //Scenario 1: staying user has no replica in moving partition - should result in a false
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser stayingUser = manager.getUserMasterById(manager.getAllUserIds().iterator().next());
        Long partitionWithoutThisUser = SparTestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        SparUser movingUser = SparTestUtils.getUserWithMasterOnPartition(manager, partitionWithoutThisUser);

        assertFalse(strategy.couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testCouldWeDeleteReplicaOfStayingUserInMovingPartitionReplicaOtherFriends() {
        //Scenario 2: staying user has a replica in moving partition, but has other friends in that partition - should result in a false
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparPartition movingPartition = manager.getPartitionById(SparTestUtils.getPartitionIdsWithNMasters(manager, 2).iterator().next());
        //Got a NoSuchElementException on the following line once
        SparUser stayingUser = manager.getUserMasterById(movingPartition.getIdsOfReplicas().iterator().next());
        SparUser movingUser = manager.getUserMasterById(movingPartition.getIdsOfMasters().iterator().next());

        Set<Long> otherUsersOnMovingPartition = new HashSet<Long>(movingPartition.getIdsOfMasters());
        otherUsersOnMovingPartition.remove(movingUser.getId());
        SparUser otherUserOnMovingPartition = manager.getUserMasterById(otherUsersOnMovingPartition.iterator().next());
        manager.befriend(otherUserOnMovingPartition, stayingUser);

        Long partitionIdWithoutStayingUser = SparTestUtils.getPartitionsWithNoPresence(manager, stayingUser.getId()).iterator().next();
        manager.addReplica(stayingUser, partitionIdWithoutStayingUser);

        assertFalse(strategy.couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }

    @Test
    public void testCouldWeDeleteReplicaOfStayingUserInMovingPartitionNoOtherFriends() {
        //Scenario 3: staying user has a replica in moving partition, and no other friends in that partition - should result in a true
        SparManager manager = SparTestUtils.getStandardManager();
        SparBefriendingStrategy strategy = new SparBefriendingStrategy(manager);

        SparUser stayingUser = manager.getUserMasterById(manager.getAllUserIds().iterator().next());
        SparUser movingUser = SparTestUtils.getUserWithMasterOnPartition(manager, stayingUser.getReplicaPartitionIds().iterator().next());

        assertTrue(strategy.couldWeDeleteReplicaOfStayingUserInMovingPartition(movingUser, stayingUser));
    }
}