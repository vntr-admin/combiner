package io.vntr.repartition;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;

import static java.util.Arrays.sort;

/**
 * Created by robertlindquist on 4/24/17.
 */
public class MetisRepartitioner {
    public static TIntIntMap partition(String commandLiteral, String tempDir, TIntObjectMap<TIntSet> tFriendships, TIntSet tPids) {
        int numPartitions = tPids.size();
        try {
            String inputFile = tempDir + File.separator + "e_pluribus_unum__" + System.nanoTime() + ".txt";
            String outputFile = inputFile + ".part." + numPartitions;

            TIntIntMap reverseUidMap = new TIntIntHashMap(tFriendships.size()+1);
            TIntIntMap mapping = getTranslationToZN(tFriendships.keys(), reverseUidMap);

            TIntObjectMap<TIntSet> translatedFriendships = translateFriendshipsToZNBased(tFriendships, mapping);
            writeAdjacencyGraphMetisStyle(translatedFriendships, inputFile);

            TIntIntMap results = innerPartition(commandLiteral, outputFile, inputFile, numPartitions, tFriendships.size());

            TIntIntMap reversePidMap = getReversePidMap(tPids);
            TIntIntMap translatedResults = translatePartitioningFromZNBased(results, reverseUidMap, reversePidMap);

            return translatedResults;
        } catch(Exception e) {
            return null;
        }
    }

    private static TIntIntMap getReversePidMap(TIntSet pids) {
        int[] sortedPids = pids.toArray();
        sort(sortedPids);

        TIntIntMap reversePidMap = new TIntIntHashMap(pids.size()+1);
        int count = 0;
        for(int i=0; i<sortedPids.length; i++) {
            reversePidMap.put(count++, sortedPids[i]);
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
        int[] sortedUids = friendships.keys();
        sort(sortedUids);
        PrintWriter pw = new PrintWriter(filename);

        pw.println(friendships.size() + " " + getNumEdges(friendships));

        for(int i=0; i<sortedUids.length; i++) {
            int next = sortedUids[i];
            TIntSet friends = friendships.get(next);
            String line = formatLine(friends);
            pw.println(line);
        }
        pw.close();
    }

    private static TIntIntMap getTranslationToZN(int[] uids, TIntIntMap reverseMap) {
        sort(uids);
        TIntIntMap dict = new TIntIntHashMap(uids.length+1);
        int index = 1;
        for(int i=0; i<uids.length; i++) {
            int nextUid = uids[i];
            dict.put(nextUid, index);
            reverseMap.put(index, nextUid);
            index++;
        }
        return dict;
    }

    private static TIntObjectMap<TIntSet> translateFriendshipsToZNBased(TIntObjectMap<TIntSet> originalFriendships, TIntIntMap mapping) {
        TIntObjectMap<TIntSet> translated = new TIntObjectHashMap<>();
        for(int i : originalFriendships.keys()) {
            TIntSet translatedFriends = new TIntHashSet(originalFriendships.get(i).size()+1);
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
        int[] sortedFriendIds = friends.toArray();
        sort(sortedFriendIds);
        StringBuilder builder = new StringBuilder();

        for(int i=0; i<sortedFriendIds.length; i++) {
            int friendId = sortedFriendIds[i];
            builder.append(friendId);
            if(i < sortedFriendIds.length-1) {
                builder.append(' ');
            }
        }
        return builder.toString();
    }
}
