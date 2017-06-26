package io.vntr.repartition;

import gnu.trove.iterator.TShortIterator;
import gnu.trove.map.TShortShortMap;
import gnu.trove.map.TShortObjectMap;
import gnu.trove.map.hash.TShortShortHashMap;
import gnu.trove.map.hash.TShortObjectHashMap;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Scanner;

import static java.util.Arrays.sort;

/**
 * Created by robertlindquist on 4/24/17.
 */
public class MetisRepartitioner {
    public static TShortShortMap partition(String commandLiteral, String tempDir, TShortObjectMap<TShortSet> tFriendships, TShortSet tPids) {
        int numPartitions = tPids.size();
        try {
            String inputFile = tempDir + File.separator + "e_pluribus_unum__" + System.nanoTime() + ".txt";
            String outputFile = inputFile + ".part." + numPartitions;

            TShortShortMap reverseUidMap = new TShortShortHashMap(tFriendships.size()+1);
            TShortShortMap mapping = getTranslationToZN(tFriendships.keys(), reverseUidMap);
            TShortObjectMap<TShortSet> translatedFriendships = translateFriendshipsToZNBased(tFriendships, mapping);
            writeAdjacencyGraphMetisStyle(translatedFriendships, inputFile);

            TShortShortMap results = innerPartition(commandLiteral, outputFile, inputFile, numPartitions, tFriendships.size());

            TShortShortMap reversePidMap = getReversePidMap(tPids);
            TShortShortMap translatedResults = translatePartitioningFromZNBased(results, reverseUidMap, reversePidMap);

            return translatedResults;
        } catch(Exception e) {
            return null;
        }
    }

    private static TShortShortMap getReversePidMap(TShortSet pids) {
        short[] sortedPids = pids.toArray();
        sort(sortedPids);

        TShortShortMap reversePidMap = new TShortShortHashMap(pids.size()+1);
        short count = 0;
        for(int i=0; i<sortedPids.length; i++) {
            reversePidMap.put(count, sortedPids[i]);
        }

        return reversePidMap;
    }

    private static TShortShortMap innerPartition(String commandLiteral, String outputFile, String inputFile, int numPartitions, int numUsers) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(commandLiteral, inputFile, "" + numPartitions);
        Process p = pb.start();
        p.waitFor();
        Scanner scanner = new Scanner(new File(outputFile));
        TShortShortMap results = new TShortShortHashMap(numUsers+1);
        short count = 1;
        while(scanner.hasNextLine()) {
            results.put(count++, Short.parseShort(scanner.nextLine()));
        }
        scanner.close();
        return results;
    }

    private static void writeAdjacencyGraphMetisStyle(TShortObjectMap<TShortSet> friendships, String filename) throws FileNotFoundException {
        short[] sortedUids = friendships.keys();
        sort(sortedUids);
        PrintWriter pw = new PrintWriter(filename);

        pw.println(friendships.size() + " " + getNumEdges(friendships));

        for(int i=0; i<sortedUids.length; i++) {
            short next = sortedUids[i];
            TShortSet friends = friendships.get(next);
            String line = formatLine(friends);
            pw.println(line);
        }
        pw.close();
    }

    private static TShortShortMap getTranslationToZN(short[] uids, TShortShortMap reverseMap) {
        sort(uids);
        TShortShortMap dict = new TShortShortHashMap(uids.length+1);
        short index = 1;
        for(short i=0; i<uids.length; i++) {
            short nextUid = uids[i];
            dict.put(nextUid, index);
            reverseMap.put(index, nextUid);
            index++;
        }
        return dict;
    }

    private static TShortObjectMap<TShortSet> translateFriendshipsToZNBased(TShortObjectMap<TShortSet> originalFriendships, TShortShortMap mapping) {
        TShortObjectMap<TShortSet> translated = new TShortObjectHashMap<>();
        for(short i : originalFriendships.keys()) {
            TShortSet translatedFriends = new TShortHashSet(originalFriendships.get(i).size()+1);
            for(TShortIterator iter = originalFriendships.get(i).iterator(); iter.hasNext(); ) {
                translatedFriends.add(mapping.get(iter.next()));
            }
            translated.put(mapping.get(i), translatedFriends);
        }
        return translated;
    }

    private static TShortShortMap translatePartitioningFromZNBased(TShortShortMap zNBasedPartitioning, TShortShortMap reverseUidMap, TShortShortMap reversePidMap) {
        TShortShortMap translatedPartitioning = new TShortShortHashMap(zNBasedPartitioning.size()+1);
        for(short i : zNBasedPartitioning.keys()) {
            short rawPid = zNBasedPartitioning.get(i);
            short translatedUid = reverseUidMap.get(i);
            short translatedPid = reversePidMap.get(rawPid);
            translatedPartitioning.put(translatedUid, translatedPid);
        }
        return translatedPartitioning;
    }

    static int getNumEdges(TShortObjectMap<TShortSet> friendships) {
        int count = 0;
        for(short uid : friendships.keys()) {
            count += friendships.get(uid).size();
        }
        return count >> 1;
    }

    static String formatLine(TShortSet friends) {
        short[] sortedFriendIds = friends.toArray();
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
