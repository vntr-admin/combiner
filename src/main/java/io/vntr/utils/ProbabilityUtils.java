package io.vntr.utils;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.list.TShortList;
import gnu.trove.list.array.TShortArrayList;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.util.*;

import static java.util.Collections.nCopies;

public class ProbabilityUtils
{
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

    private static final Map<DoublePair, LogNormalDistribution> lndMap = new HashMap<>();

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

    //Take a LogNormal distribution with mean=12.08... and std. dev=0.4463... and sample (index) times at random, returning the largest of those samples.
    //Run this many, many times, average it out, and you get the following.
    private static final double[] LND_12_08441436510468D__0_44631395858726847 = {
            0.0,                195657.98632876502, 244020.6328283635,  273394.99904716003, 294988.0847703082,
            311672.25452848163, 325320.2732446085,  337328.75668360095, 347380.1512902926,  356549.39208677714,
            364630.39411803795, 372044.69734810933, 378916.3499029386,  385082.61067727854, 391045.2072235034,
            396527.31083416543, 401543.7168287262,  406536.7418948179,  410949.7737790831,  415213.67497016274,
            419372.81195045833, 423337.15777956555, 426968.3675459797,  430357.397443473,   434038.0271489305,
            437353.5358557809,  440522.0621962701,  443497.7797050301,  446621.614580313,   449451.1814124973,
            452060.95039437246, 454791.88042708876, 457385.2772394902,  459961.4216538926,  462390.94630670256,
            464634.0052529193,  467062.67795029853, 469158.6891991368,  471335.58885570156, 473707.1699430232,
            475545.5987750892,  477643.3974954986,  479516.84218630823, 481569.1424085323,  483508.0149838952,
            485438.2736777312,  487526.0810773428,  488921.7773349877,  490896.64681053115, 492281.76678171847,
            494325.54941423144, 495997.58377810585, 497595.03435681394, 499108.7946891761,  500575.939904589,
            502115.47498336097, 503685.01420118986, 505168.59553250315, 506470.0442255406,  508006.9531594901,
            509443.3483519527,  510771.6580850213,  512201.79950572684, 513390.67327302153, 514976.7343427698,
            516239.5603975123,  517244.7518018421,  518649.93865231704, 519866.0271824818,  521387.8217391491,
            522345.890881855,   523843.64767330646, 524652.5648374663,  525948.1267343157,  527121.2357251248,
            528199.2495746213,  529475.9483512825,  530513.6256185379,  531353.2366312597,  532804.7661783479,
            533469.3639357516,  534827.0353783002,  535849.0865631071,  537157.0400299573,  537833.1952765761,
            538817.2061724439,  539662.8440269926,  540870.5787358591,  541833.420237187,   542876.3089167847,
            543780.2008518635,  544829.4346415402,  545697.4696847259,  546496.9543632029,  547435.7278221811,
            548321.3387962918,  549300.292414324,   550275.4315798462,  551047.3528696031,  551870.9824820156,
            552810.7058726769,  553662.6757579317,  554393.121858875,   555498.2646963458,  556172.209173149,
            556856.2528424311,  557704.999444486,   558528.8744648766,  559505.1579294016,  560241.9871885503,
            561039.8286756055,  561722.6927021997,  562646.9013963868,  563329.1391792656,  564061.250347869,
            564912.5072548813,  565618.2254210257,  566380.8192747955,  567075.4219889867,  568069.3912965661,
            568676.6059607276,  569355.2468904436,  570028.8304139175,  570701.189089678,   571397.0182352932,
            572019.1383384631,  572760.8003005937,  573472.2927654036,  574102.3443944044,  574813.3402062273,
            575368.1120143331,  576299.9055748311,  576732.9520505958,  577653.4942470673,  578028.072513679,
            578733.0783904593,  579392.0854098672,  579922.934888735,   580763.4640462777,  581363.8573057498,
            582050.0960151738,  582621.5322145673,  583266.7566079606,  583915.9709507131,  584585.1497357495,
            584926.016313138,   585488.6500278158,  586347.3969424524,  586787.3278053445,  587533.0155580442,
            587905.3820659143
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

    public static double calculateExpectedQueryDelay(TShortObjectMap<TShortSet> friendships, TShortObjectMap<TShortSet> partitions) {
        TShortShortMap uidToPidMap = TroveUtils.getUToMasterMap(partitions);
        TShortObjectMap<TShortSet> uidToFriendPidsMap = new TShortObjectHashMap<>(friendships.size()+1);
        for(short uid : friendships.keys()) {
            uidToFriendPidsMap.put(uid, new TShortHashSet());
        }

        for(short uid : friendships.keys()) {
            for(TShortIterator iter = friendships.get(uid).iterator(); iter.hasNext(); ) {
                short friendId = iter.next();
                if(uid < friendId) {
                    short pid = uidToPidMap.get(uid);
                    short friendPid = uidToPidMap.get(friendId);
                    uidToFriendPidsMap.get(uid).add(friendPid);
                    uidToFriendPidsMap.get(friendId).add(pid);
                }
            }
        }

        double sum = 0;
        for(short uid : friendships.keys()) {
            double expectedDelay = LND_12_08441436510468D__0_44631395858726847[uidToFriendPidsMap.get(uid).size()];
            sum += expectedDelay;
        }

        return sum / friendships.size();
    }

	public static double calculateAssortivityCoefficient(TShortObjectMap<TShortSet> bidirectionalFriendships) {
		//We calculate "the Pearson correlation coefficient of the degrees at either ends of an edge"
        //Newman, M. E. (2002). Assortative mixing in networks. Physical review letters, 89(20), 208701.
        //This is also called "Degree-Centrality Assortativity"
        int edgeCount = 0;
        for(TShortSet friends : bidirectionalFriendships.valueCollection()) {
            edgeCount += friends.size();
        }
        double[] x = new double[edgeCount];
        double[] y = new double[edgeCount];
        int i=0;
        for(short uid1 : bidirectionalFriendships.keys()) {
            for(TShortIterator iter = bidirectionalFriendships.get(uid1).iterator(); iter.hasNext(); ) {
                short uid2 = iter.next();
                x[i] = bidirectionalFriendships.get(uid1).size();
                y[i] = bidirectionalFriendships.get(uid2).size();
                i++;
            }
        }

        return new PearsonsCorrelation().correlation(x, y);
	}

	public static short chooseKeyFromMapSetInProportionToSetSize(TShortObjectMap<TShortSet> mapset) {
        TShortList keys = new TShortArrayList(mapset.size() * 100);
        for(short key : mapset.keys()) {
            //this is intentional: we want mapset.get(key).size() copies of key in there
            keys.addAll(nCopies(mapset.get(key).size(), key));
        }

        return keys.get((int)(Math.random() * keys.size()));
    }

    public static List<Short> chooseKeyValuePairFromMapSetUniformly(TShortObjectMap<TShortSet> mapset) {
        short key = chooseKeyFromMapSetInProportionToSetSize(mapset);
        short value = TroveUtils.getRandomElement(mapset.get(key));
        return Arrays.asList(key, value);
    }
}
