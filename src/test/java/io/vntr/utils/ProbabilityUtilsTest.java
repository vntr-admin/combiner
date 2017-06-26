package io.vntr.utils;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import org.junit.Test;

import static io.vntr.utils.TroveUtils.initSet;

/**
 * Created by robertlindquist on 1/18/17.
 */
public class ProbabilityUtilsTest {

    @Test
    public void testCalculateDegreeCentralityAssortivityCoefficient() {
        TShortObjectMap<TShortSet> friendships = new TShortObjectHashMap<>();
        friendships.put((short) 0, initSet(1, 2, 3));
        friendships.put((short) 1, initSet(0, 2));
        friendships.put((short) 2, initSet(0, 1, 3));
        friendships.put((short) 3, initSet(0, 2, 4));
        friendships.put((short) 4, initSet(3, 5));
        friendships.put((short) 5, initSet(4, 6, 7));
        friendships.put((short) 6, initSet(5, 7));
        friendships.put((short) 7, initSet(5, 6));

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
