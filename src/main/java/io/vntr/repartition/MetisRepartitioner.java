package io.vntr.repartition;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by robertlindquist on 4/24/17.
 */
public class MetisRepartitioner {
    public static Map<Integer, Integer> partition(String commandLiteral, String tempDir, Map<Integer, Set<Integer>> friendships, Set<Integer> pids) {
        int numPartitions = pids.size();
        try {
            String inputFile = tempDir + File.separator + "e_pluribus_unum__" + System.nanoTime() + ".txt";
            String outputFile = inputFile + ".part." + numPartitions;

            Map<Integer, Integer> reverseUidMap = new HashMap<>();
            Map<Integer, Integer> mapping = getTranslationToZN(friendships.keySet(), reverseUidMap);
            Map<Integer, Set<Integer>> translatedFriendships = translateFriendshipsToZNBased(friendships, mapping);
            writeAdjacencyGraphMetisStyle(translatedFriendships, inputFile);

            Map<Integer, Integer> results = innerPartition(commandLiteral, outputFile, inputFile, numPartitions);

            Map<Integer, Integer> reversePidMap = getReversePidMap(pids);
            Map<Integer, Integer> translatedResults = translatePartitioningFromZNBased(results, reverseUidMap, reversePidMap);

            return translatedResults;
        } catch(Exception e) {
            return null;
        }
    }

    private static Map<Integer, Integer> getReversePidMap(Set<Integer> pids) {
        Map<Integer, Integer> reversePidMap = new HashMap<>();
        NavigableSet<Integer> navPids = new TreeSet<>(pids);
        int count = 0;
        for(Iterator<Integer> iter = navPids.iterator(); iter.hasNext(); count++) {
            reversePidMap.put(count, iter.next());
        }

        return reversePidMap;
    }

    private static Map<Integer, Integer> innerPartition(String commandLiteral, String outputFile, String inputFile, int numPartitions) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(commandLiteral, inputFile, "" + numPartitions);
        Process p = pb.start();
        p.waitFor();
        Scanner scanner = new Scanner(new File(outputFile));
        Map<Integer, Integer> results = new HashMap<>();
        int count = 1;
        while(scanner.hasNextLine()) {
            results.put(count++, Integer.parseInt(scanner.nextLine()));
        }
        scanner.close();
        return results;
    }

    private static void writeAdjacencyGraphMetisStyle(Map<Integer, Set<Integer>> friendships, String filename) throws FileNotFoundException {
        NavigableMap<Integer, Set<Integer>> navMap = new TreeMap<>(friendships);
        PrintWriter pw = new PrintWriter(filename);

        pw.println(friendships.size() + " " + getNumEdges(friendships));

        for(Iterator<Integer> iter = navMap.keySet().iterator(); iter.hasNext(); ) {
            int next = iter.next();
            Set<Integer> friends = friendships.get(next);
            String line = formatLine(friends);
            pw.println(line);
        }
        pw.close();
    }

    private static Map<Integer, Integer> getTranslationToZN(Set<Integer> keys, Map<Integer, Integer> reverseMap) {
        NavigableSet<Integer> sortedKeys = new TreeSet<>(keys);
        Map<Integer, Integer> dict = new HashMap<>();
        int i = 1;
        for(Iterator<Integer> iter = sortedKeys.iterator(); iter.hasNext(); i++) {
            int next = iter.next();
            dict.put(next, i);
            reverseMap.put(i, next);
        }
        return dict;
    }

    private static Map<Integer, Set<Integer>> translateFriendshipsToZNBased(Map<Integer, Set<Integer>> originalFriendships, Map<Integer, Integer> mapping) {
        Map<Integer, Set<Integer>> translated = new HashMap<>();
        for(int i : originalFriendships.keySet()) {
            Set<Integer> translatedFriends = new HashSet<>();
            for(int friendId : originalFriendships.get(i)) {
                translatedFriends.add(mapping.get(friendId));
            }
            translated.put(mapping.get(i), translatedFriends);
        }
        return translated;
    }

    private static Map<Integer, Integer> translatePartitioningFromZNBased(Map<Integer, Integer> zNBasedPartitioning, Map<Integer, Integer> reverseUidMap, Map<Integer, Integer> reversePidMap) {
        Map<Integer, Integer> translatedPartitioning = new HashMap<>();
        for(int i : zNBasedPartitioning.keySet()) {
            int rawPid = zNBasedPartitioning.get(i);
            int translatedUid = reverseUidMap.get(i);
            int translatedPid = reversePidMap.get(rawPid);
            translatedPartitioning.put(translatedUid, translatedPid);
        }
        return translatedPartitioning;
    }

    static int getNumEdges(Map<Integer, Set<Integer>> friendships) {
        int count = 0;
        for(Set<Integer> set : friendships.values()) {
            count += set.size();
        }
        return count >> 1;
    }

    static String formatLine(Set<Integer> friends) {
        StringBuilder builder = new StringBuilder();
        for(Iterator<Integer> iter = new TreeSet<>(friends).iterator(); iter.hasNext(); ) {
            builder.append((iter.next()));
            if(iter.hasNext()) {
                builder.append(' ');
            }
        }
        return builder.toString();
    }
}
