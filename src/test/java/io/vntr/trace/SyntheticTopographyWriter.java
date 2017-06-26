package io.vntr.trace;

import gnu.trove.map.TShortObjectMap;
import gnu.trove.set.TShortSet;
import io.vntr.utils.ProbabilityUtils;
import io.vntr.utils.TroveUtils;
import org.junit.Test;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

import static io.vntr.TestUtils.getTopographyForMultigroupSocialNetwork;
import static io.vntr.utils.TroveUtils.convert;

/**
 * Created by robertlindquist on 1/19/17.
 */
public class SyntheticTopographyWriter {

    private static final int NUM_USERS     = 70000;
    private static final int NUM_GROUPS    = 1500;
    private static final float GROUP_PROB  = 0.0022f;
    private static final float FRIEND_PROB = 0.055f;
    private static final String OUTPUT_DIR = "/Users/robertlindquist/Documents/thesis/data/synthetic/";

    private static final String probFormat = "%s";
    private static final String filnameFormat = OUTPUT_DIR + "synth_u%d_f%d_g%d_gp_%s_fp_%s_%s.txt";

    @Test
    public void generateIt() throws Exception {
        double maxAssortivity = Double.MIN_VALUE;
        TShortObjectMap<TShortSet> highestAssortivityFriendships = null;
        for(int i=0; i<1000; i++) {
            System.out.println(i);
            TShortObjectMap<TShortSet> friendships = getTopographyForMultigroupSocialNetwork(NUM_USERS, NUM_GROUPS, GROUP_PROB, FRIEND_PROB);
            printStatistics(friendships);
            double assortivity = ProbabilityUtils.calculateAssortivityCoefficient(friendships);
            if(assortivity > maxAssortivity) {
                maxAssortivity = assortivity;
                highestAssortivityFriendships = friendships;
            }
        }
        System.out.println();
        System.out.println();

        printStatistics(highestAssortivityFriendships);
        String filename = getFilename(countFriendships(highestAssortivityFriendships)/2);
        saveFriendshipsToFile(highestAssortivityFriendships, filename);
    }

    private static String getFilename(int numF) {
        String formattedGroupProb  = String.format(probFormat, GROUP_PROB);
        String groupProb = formattedGroupProb.replaceAll("\\.", "_");

        String formattedFriendProb = String.format(probFormat, FRIEND_PROB);
        String friendProb = formattedFriendProb.replaceAll("\\.", "_");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = sdf.format(new Date());

        return String.format(filnameFormat, NUM_USERS, numF, NUM_GROUPS, groupProb, friendProb, date);
    }

    public static void saveFriendshipsToFile(TShortObjectMap<TShortSet> friendships, String filename) throws Exception {
        PrintWriter pw = new PrintWriter(filename);
        for(Iterator<Short> iter = getIter(convert(friendships.keySet())); iter.hasNext(); ) {
            StringBuilder builder = new StringBuilder();
            short uid = iter.next();
            builder.append(uid);
            builder.append(": ");
            for(Iterator<Short> innerIter = getIter(convert(friendships.get(uid))); innerIter.hasNext(); ) {
                int friendId = innerIter.next();
                if(friendId < uid) {
                    continue;
                }
                builder.append(friendId);
                if(innerIter.hasNext()) {
                    builder.append(", ");
                }
            }
            pw.println(builder.toString());
            pw.flush();
        }
        pw.close();
    }

    private static Iterator<Short> getIter(Set<Short> set) {
        return new TreeSet<>(set).iterator();
    }

    private static int countFriendships(TShortObjectMap<TShortSet> friendships) {
        int numF = 0;
        for(short uid : friendships.keys()) {
            numF += friendships.get(uid).size();
        }
        return numF;
    }

    static void printStatistics(TShortObjectMap<TShortSet> friendships) {
        TShortObjectMap<TShortSet> bidirectionalFriendships = TroveUtils.generateBidirectionalFriendshipSet(friendships);

        int numU = friendships.size();
        int numF = 0;
        NavigableSet<Integer> numFriends = new TreeSet<>();
        for(short uid : friendships.keys()) {
            numF += friendships.get(uid).size();
            numFriends.add(bidirectionalFriendships.get(uid).size());
        }
        numF /= 2;
        double assortivity = ProbabilityUtils.calculateAssortivityCoefficient(friendships);

        Iterator<Integer> descNumFriendsIter = numFriends.descendingIterator();
        int highestNumFriendships = descNumFriendsIter.next();
        int secondHighestNumFriendships = descNumFriendsIter.next();
        int thirdHighestNumFriendships = descNumFriendsIter.next();

        String statisticsPrintFormat = "#U: %5d, #F: %6d, Assortivity: %1.8f, top 3 friendships: (%4d, %4d, %4d)%n";

        System.out.printf(statisticsPrintFormat, numU, numF, assortivity, highestNumFriendships, secondHighestNumFriendships, thirdHighestNumFriendships);
    }


}
