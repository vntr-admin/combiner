package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;
import io.vntr.utils.TroveUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by robertlindquist on 4/24/17.
 */
public class MetisRepartitioner {
    public static TIntIntMap partition(String commandLiteral, String tempDir, TIntObjectMap<TIntSet> friendships, TIntSet pids) {
        int numPartitions = pids.size();
        try {
            String inputFile = tempDir + File.separator + "e_pluribus_unum__" + System.nanoTime() + ".txt";
            String outputFile = inputFile + ".part." + numPartitions;

            TIntIntMap reverseUidMap = new TIntIntHashMap((int)(friendships.size() * 1.1));
            TIntIntMap mapping = getTranslationToZN(friendships.keySet(), reverseUidMap);
            TIntObjectMap<TIntSet> translatedFriendships = translateFriendshipsToZNBased(friendships, mapping);
            writeAdjacencyGraphMetisStyle(translatedFriendships, inputFile);

            TIntIntMap results = innerPartition(commandLiteral, outputFile, inputFile, numPartitions, friendships.size());

            TIntIntMap reversePidMap = getReversePidMap(pids);
            TIntIntMap translatedResults = translatePartitioningFromZNBased(results, reverseUidMap, reversePidMap);

            return translatedResults;
        } catch(Exception e) {
            return null;
        }
    }

    private static TIntIntMap getReversePidMap(TIntSet pids) {
        int[] sortedArray = pids.toArray();
        Arrays.sort(sortedArray);

        TIntIntMap reversePidMap = new TIntIntHashMap(pids.size()+1);
        int count = 0;
        for(int i=0; i<sortedArray.length; i++) {
            reversePidMap.put(count, sortedArray[i]);
        }

        return reversePidMap;
    }

    private static TIntIntMap innerPartition(String commandLiteral, String outputFile, String inputFile, int numPartitions, int numUsers) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(commandLiteral, inputFile, "" + numPartitions);
        Process p = pb.start();
        p.waitFor();
        Scanner scanner = new Scanner(new File(outputFile));
        TIntIntMap results = new TIntIntHashMap(numUsers+1);
        int count = 1;
        while(scanner.hasNextLine()) {
            results.put(count++, Integer.parseInt(scanner.nextLine()));
        }
        scanner.close();
        return results;
    }

    private static void writeAdjacencyGraphMetisStyle(TIntObjectMap<TIntSet> friendships, String filename) throws FileNotFoundException {
        int[] sortedArray = friendships.keys();
        Arrays.sort(sortedArray);

        PrintWriter pw = new PrintWriter(filename);
        pw.println(friendships.size() + " " + getNumEdges(friendships));

        for(int i=0; i<sortedArray.length; i++) {
            TIntSet friends = friendships.get(sortedArray[i]);
            String line = formatLine(friends);
            pw.println(line);
        }
        pw.close();
    }

    private static TIntIntMap getTranslationToZN(TIntSet keys, TIntIntMap reverseMap) {
        int[] sortedArray = keys.toArray();
        Arrays.sort(sortedArray);
        TIntIntMap dict = new TIntIntHashMap(sortedArray.length+1);
        for(int i=0; i<sortedArray.length; i++) {
            int next = sortedArray[i];
            dict.put(next, i);
            reverseMap.put(i, next);
        }
        return dict;
    }

    private static TIntObjectMap<TIntSet> translateFriendshipsToZNBased(TIntObjectMap<TIntSet> originalFriendships, TIntIntMap mapping) {
        TIntObjectMap<TIntSet> translated = new TIntObjectHashMap<>(mapping.size()+1);
        for(int i : originalFriendships.keys()) {
            TIntSet translatedFriends = new TIntHashSet();
            for(TIntIterator iter = originalFriendships.get(i).iterator(); iter.hasNext(); ) {
                translatedFriends.add(mapping.get(iter.next()));
            }
            translated.put(mapping.get(i), translatedFriends);
        }
        return translated;
    }

    private static TIntIntMap translatePartitioningFromZNBased(TIntIntMap zNBasedPartitioning, TIntIntMap reverseUidMap, TIntIntMap reversePidMap) {
        TIntIntMap translatedPartitioning = new TIntIntHashMap(zNBasedPartitioning.size()+1);
        for(int i : zNBasedPartitioning.keys()) {
            int rawPid = zNBasedPartitioning.get(i);
            int translatedUid = reverseUidMap.get(i);
            int translatedPid = reversePidMap.get(rawPid);
            translatedPartitioning.put(translatedUid, translatedPid);
        }
        return translatedPartitioning;
    }

    static int getNumEdges(TIntObjectMap<TIntSet> friendships) {
        int count = 0;
        for(int uid : friendships.keys()) {
            count += friendships.get(uid).size();
        }
        return count >> 1;
    }

    static String formatLine(TIntSet friends) {
        int[] array = friends.toArray();
        Arrays.sort(array);
        StringBuilder builder = new StringBuilder();
        for(int i=0; i<array.length; i++) {
            builder.append(array[i]);
            if(i < array.length-1) {
                builder.append(' ');
            }
        }
        return builder.toString();
    }
}
