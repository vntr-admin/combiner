package io.vntr.utils;

import cern.jet.random.engine.DRand;
import cern.jet.random.engine.RandomEngine;
import cern.jet.random.sampling.RandomSampler;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.util.*;

public class ProbabilityUtils
{
    private static RandomEngine randomEngine = new DRand(((int) System.nanoTime()) >>> 2);

    private static class DoublePair implements Comparable<DoublePair>{
        public final Double mean;
        public final Double stdDeviation;

        public DoublePair(Double mean, Double stdDeviation) {
            this.mean = mean;
            this.stdDeviation = stdDeviation;
        }

        public Double getMean() {
            return mean;
        }

        public Double getStdDeviation() {
            return stdDeviation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DoublePair that = (DoublePair) o;

            if (!mean.equals(that.mean)) return false;
            return stdDeviation.equals(that.stdDeviation);
        }

        @Override
        public int hashCode() {
            int result = mean.hashCode();
            result = 31 * result + stdDeviation.hashCode();
            return result;
        }

        public int compareTo(DoublePair another) {
            int meanCompare = this.mean.compareTo(another.mean);
            if(meanCompare != 0) {
                return meanCompare;
            }
            return this.stdDeviation.compareTo(another.stdDeviation);
        }
    }

    private static final Map<DoublePair, LogNormalDistribution> lndMap = new HashMap<DoublePair, LogNormalDistribution>();

    //http://blog.simiacryptus.com/2015/10/modeling-network-latency.html
    public static double drawKFromLogNormalDistributionAndReturnMax(double mean, double stdDeviation, int k) {
        DoublePair key = new DoublePair(mean, stdDeviation);
        LogNormalDistribution logNormalDistribution;
        if(lndMap.containsKey(key)) {
            logNormalDistribution = lndMap.get(key);
        }
        else {
            logNormalDistribution = new LogNormalDistribution(mean, stdDeviation);
            if(lndMap.size() < 100) {
                lndMap.put(key, logNormalDistribution);
            }
        }
        double max = Double.MIN_VALUE;
        for(int i=0; i<k; i++) {
            max = Math.max(max, logNormalDistribution.sample());
        }
        return max;

    }

    public static double drawFromLogNormalDistribution(double mean, double stdDeviation) {
        return drawKFromLogNormalDistributionAndReturnMax(mean, stdDeviation, 1);
    }

	public static Set<Integer> getKDistinctValuesBetweenMandNInclusive(int k, int m, int n)
	{
		List<Integer> tempList = new LinkedList<Integer>();
		for(int i = m; i <= n; i++)
		{
			tempList.add(i);
		}

		return getKDistinctValuesFromList(k, tempList);
	}

	public static int getRandomElement(Collection<Integer> set) {
        return getKDistinctValuesFromList(1, new LinkedList<Integer>(set)).iterator().next();
    }

	public static Set<Integer> getKDistinctValuesFromList(int k, Collection<Integer> set)
	{
		return getKDistinctValuesFromList(k, new LinkedList<Integer>(set));
	}

	public static Set<Integer> getKDistinctValuesFromList(int k, List<Integer> list)
	{
	    long[] indices = new long[k];
        RandomSampler.sample(k, list.size(), k, 0, indices, 0, randomEngine);

		Set<Integer> returnSet = new HashSet<Integer>();
		for(long index : indices) {
		    returnSet.add(list.get((int) index));
        }
		return returnSet;
	}

    public static Map<Integer, Set<Integer>> generateBidirectionalFriendshipSet(Map<Integer, Set<Integer>> friendships) {
        Map<Integer, Set<Integer>> bidirectionalFriendshipSet = new HashMap<Integer, Set<Integer>>();
        for(Integer uid : friendships.keySet()) {
            bidirectionalFriendshipSet.put(uid, new HashSet<Integer>());
        }
        for(Integer uid1 : friendships.keySet()) {
            for(Integer uid2 : friendships.get(uid1)) {
                bidirectionalFriendshipSet.get(uid1).add(uid2);
                bidirectionalFriendshipSet.get(uid2).add(uid1);
            }
        }
        return bidirectionalFriendshipSet;
    }

    //Take a LogNormal distribution with mean=12.08... and std. dev=0.4463... and sample (index) times at random, returning the largest of those samples.
    //Run this many, many times, average it out, and you get the following.
    private static final double[] LND_12_08441436510468D__0_44631395858726847 = {
            0,                  195676.31940628562, 244053.13131996954, 273562.667483572,   294702.8522283689,
            311608.212897449,   325349.4443799436,  337164.3154876691,  347420.2067972925,  356459.8860661987,
            364752.5877304666,  372152.61177909275, 378654.1889939618,  385079.6274437595,  391018.8028193601,
            396654.2319797923,  401573.13997983414, 406571.75414291635, 410943.63979241607, 415258.87312520447,
            419354.9626633714,  423205.55944505805, 426942.12156073254, 430717.0322897189,  434178.1045532156,
            437414.48653889936, 440439.66368555895, 443674.90253123356, 446754.5492038839,  449219.9549287621,
            452032.0211453834,  454867.18222781445, 457486.38298993313, 459994.9266175493,  462316.4961519276,
            464612.8134445291,  466898.1521577051,  469333.16271817824, 471405.43488899723, 473507.21630327584,
            475783.49536383094, 477754.7097066543,  479694.1776055902,  481465.0805045318,  483551.8883741623,
            485388.4090461885,  487283.981522448,   489031.07620496384, 490612.64003284875, 492414.9995281722,
            494185.9478328977,  495779.4319007987,  497321.3739874531,  499273.1642794827,  500716.4465337309,
            502124.0865181631,  503638.56730975496, 505214.54509488324, 506611.59413997945, 507940.7190761551,
            509518.0561256115,  510769.2863881082,  512157.24697139324, 513572.4332766364,  514791.7281956562,
            516048.32752895943, 517478.9573888674,  518568.6981394038,  519978.21095086675, 521303.96466302447,
            522513.235883451,   523591.78353995545, 524705.2527983639,  525849.2638430938,  527017.4953275218,
            527937.1710614645,  529450.6778925523,  530478.9194232662,  531372.1533829551,  532815.0971521623,
            533637.9018183503,  534705.8954295015,  535591.0732376686,  536654.1569289089,  537905.2772786389,
            538901.2331595342,  539830.1085454824,  541105.8826251094,  541878.8815527632,  542833.0376619411,
            543827.3271695902,  544566.1689727835,  545582.9201718714,  546523.6964281865,  547515.8781239025,
            548460.1770662266,  549216.4848519898,  550341.3337232049,  551361.5413308092,  551885.284320471,
            552778.1441541595
    };

    //Same as above, but with mean=11.78... and std. dev=1.776...
    private static final double[] LND_11_784808492653509__1_7764520481053447 = {
            0,                  633932.8736364115,  1145820.0389870468, 1580414.9557458216, 1964113.8710114069,
            2325591.649478559,  2650582.5162054384, 2978547.570026696,  3270196.954378095,  3554028.3636682173,
            3809961.5048935534, 4091661.4845457985, 4351829.333201673,  4588810.07489811,   4813858.589590206,
            5025128.757953739,  5258019.664351835,  5468140.874391797,  5678017.333205334,  5884235.04123057,
            6109432.564547575,  6272194.527764348,  6461675.949044348,  6660580.551665375,  6852955.454773206,
            7031329.80073689,   7205044.469582153,  7384275.345903573,  7540581.541579517,  7725801.660757148,
            7913466.5641963985, 8048648.616446856,  8197098.468017235,  8368793.111996623,  8520052.792212706,
            8692421.834206091,  8813809.75175853,   8962049.226912633,  9128025.724804908,  9295057.332028706,
            9424136.962014198,  9552150.703963563,  9709727.110077742,  9845360.194556754,  9964033.151316555,
            1009882.6041357808, 1023055.1749477461, 1034082.6560530936, 1052591.0309329296, 1062197.8600299064,
            1075380.5137916813, 1090457.1175575664, 1100581.0938455736, 1117999.5725519802, 1128517.0015605932,
            1140113.893652187,  1150559.1465639403, 1167360.5399613105, 1172618.403086237,  1186482.6701867698,
            1196380.3097102163, 1212495.9320080878, 1225446.6484363291, 1235286.8017797206, 1246114.3110666431,
            1254613.3411575494, 1268277.4169173775, 1281288.2742017144, 1289306.81306709,   1300683.1882961495,
            1309813.0090399295, 1321184.3081134265, 1329415.1443891345, 1346564.977327967,  1356920.6347894082,
            1362847.5755905967, 1373003.1061016617, 1385623.7428321738, 1396412.2273970487, 1405220.3038087035,
            1418761.0864965836, 1425054.137301217,  1435282.3646107953, 1450938.5474394111, 1454660.7714316288,
            1467276.1713571105, 1478269.7639368463, 1484129.8366253354, 1494912.972470405,  1501949.3268295195,
            1513327.8565179475, 1524303.4652639193, 1531243.6869552348, 1544623.5959233413, 1553004.9818304185,
            1564490.1495150505, 1569866.7476530334, 1579449.7775729708, 1591588.9235795947, 1598744.8116838,
            1608847.201841079
    };

    private static Map<Integer, Integer> invertPartitions(Map<Integer, Set<Integer>> partitions) {
        Map<Integer, Integer> inverse = new HashMap<Integer, Integer>();
        for(int pid : partitions.keySet()) {
            for(int uid : partitions.get(pid)) {
                inverse.put(uid, pid);
            }
        }
        return inverse;

    }

    public static double calculateExpectedQueryDelay(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) {
        Map<Integer, Integer> uidToPidMap = invertPartitions(partitions);
        Map<Integer, Set<Integer>> uidToFriendPidsMap = new HashMap<Integer, Set<Integer>>();
        for(int uid : friendships.keySet()) {
            uidToFriendPidsMap.put(uid, new HashSet<Integer>());
        }

        for(int uid : friendships.keySet()) {
            for(int friendId : friendships.get(uid)) {
                int pid = uidToPidMap.get(uid);
                int friendPid = uidToPidMap.get(friendId);
                uidToFriendPidsMap.get(uid).add(friendPid);
                uidToFriendPidsMap.get(friendId).add(pid);
            }
        }

        double sum = 0;
        for(int uid : friendships.keySet()) {
            double expectedDelay = LND_12_08441436510468D__0_44631395858726847[friendships.get(uid).size()];
            sum += expectedDelay;
        }

        return sum / friendships.size();
    }

	public static double calculateAssortivityCoefficient(Map<Integer, Set<Integer>> friendships) {
		//We calculate "the Pearson correlation coefficient of the degrees at either ends of an edge"
        //Newman, M. E. (2002). Assortative mixing in networks. Physical review letters, 89(20), 208701.
        //This is also called "Degree-Centrality Assortativity"
        Map<Integer, Set<Integer>> bidirectionalFriendships = generateBidirectionalFriendshipSet(friendships);
        int edgeCount = 0;
        for(Set<Integer> friends : friendships.values()) {
            edgeCount += friends.size();
        }
        double[] x = new double[edgeCount];
        double[] y = new double[edgeCount];
        int i=0;
        for(int uid1 : friendships.keySet()) {
            for(int uid2 : friendships.get(uid1)) {
                x[i] = bidirectionalFriendships.get(uid1).size();
                y[i] = bidirectionalFriendships.get(uid2).size();
                i++;
            }
        }

        return new PearsonsCorrelation().correlation(x, y);
	}

	public static int chooseKeyFromMapSetInProportionToSetSize(Map<Integer, Set<Integer>> mapset) {
        List<Integer> keys = new ArrayList<Integer>(mapset.size() * 100);
        for(int key : mapset.keySet()) {
            for(int i=0; i<mapset.get(key).size(); i++) {
                keys.add(key); //this is intentional: we want mapset.get(key).size() copies of key in there
            }
        }

        return ProbabilityUtils.getRandomElement(keys);
    }

    public static List<Integer> chooseKeyValuePairFromMapSetUniformly(Map<Integer, Set<Integer>> mapset) {
        int key = chooseKeyFromMapSetInProportionToSetSize(mapset);
        int value = getRandomElement(mapset.get(key));
        return Arrays.asList(key, value);
    }
}
