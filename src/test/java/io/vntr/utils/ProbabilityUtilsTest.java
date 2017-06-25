package io.vntr.utils;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import org.junit.Test;

/**
 * Created by robertlindquist on 1/18/17.
 */
public class ProbabilityUtilsTest {

    @Test
    public void testCalculateDegreeCentralityAssortivityCoefficient() {
        TIntObjectMap<TIntSet> friendships = new TIntObjectHashMap<>();
        friendships.put(0, TroveUtils.initSet(1, 2, 3));
        friendships.put(1, TroveUtils.initSet(0, 2));
        friendships.put(2, TroveUtils.initSet(0, 1, 3));
        friendships.put(3, TroveUtils.initSet(0, 2, 4));
        friendships.put(4, TroveUtils.initSet(3, 5));
        friendships.put(5, TroveUtils.initSet(4, 6, 7));
        friendships.put(6, TroveUtils.initSet(5, 7));
        friendships.put(7, TroveUtils.initSet(5, 6));

        System.out.println(ProbabilityUtils.calculateAssortivityCoefficient(friendships));
    }

    @Test
    public void testLogNormalDistributionDraw() {
        double mean = 12.08441436510468D;
        double stdDeviation = 0.44631395858726847D;
        System.out.println(ProbabilityUtils.drawFromLogNormalDistribution(mean, stdDeviation));
    }

    private static final int MIN_LND_K = 1;
    private static final int MAX_LND_K = 150;
    private static final int NUM_LND_SAMPLES = 1000000;

//    @Test
    public void testLogNormalDistributionDrawKMax() {
        double mean = 12.08441436510468D;
//        double mean = 11.784808492653509D;
        double stdDeviation = 0.44631395858726847D;
//        double stdDeviation = 1.7764520481053447D;
        for(int k=MIN_LND_K; k<=MAX_LND_K; k++) {
            double sum = 0;
            for(int j=0; j<NUM_LND_SAMPLES; j++) {
                sum += ProbabilityUtils.drawKFromLogNormalDistributionAndReturnMax(mean, stdDeviation, k);
            }
            double average = sum / NUM_LND_SAMPLES;
            System.out.println(k + ": " + average);
        }
    }

}
