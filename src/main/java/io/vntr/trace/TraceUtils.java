package io.vntr.trace;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.File;
import java.util.*;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class TraceUtils {
    public static Trace getFullTraceFromFile(String filename) {
        Trace baseTrace = null;
        List<TraceAction> actions = new LinkedList<>();
        TIntObjectMap<TIntSet> friendships = null;
        TIntSet pids = null;
        TIntObjectMap<TIntSet> partitions = null;
        TIntObjectMap<TIntSet> replicas = null;

        Scanner scanner = null;

        try {
            scanner = new Scanner(new File(filename));
            while(scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if(line.startsWith("F: ")) {
                    friendships = parseMapSetLine(line.substring(3));
                } else if(line.startsWith("PIDS: ")) {
                    pids = parseSetLine(line.substring(6));
                } else if(line.startsWith("P: ")) {
                    partitions = parseMapSetLine(line.substring(3));
                } else if(line.startsWith("R: ")) {
                    replicas = parseMapSetLine(line.substring(3));
                } else if(line.startsWith("A0")) {
                    if(friendships == null || pids == null) {
                        throw new RuntimeException("Malformed file");
                    }
                    baseTrace = new Trace();
                    baseTrace.setReplicas(replicas);
                    baseTrace.setPartitions(partitions);
                    baseTrace.setFriendships(friendships);
                    baseTrace.setActions(actions);

                    TraceAction action = TraceAction.fromString(line.substring(line.indexOf(':')+1).trim());
                    actions.add(action);

                } else if(line.startsWith("A")) {
                    TraceAction action = TraceAction.fromString(line.substring(line.indexOf(':')+1).trim());
                    actions.add(action);
                }
            }
        } catch(Exception e) {
        } finally {
            if(scanner != null) {
                scanner.close();
            }
        }
        return baseTrace;
    }

    static TIntObjectMap<TIntSet> parseMapSetLine(String line) {
        String tempLine = line.substring(1, line.length()-1);
        String[] chunks = tempLine.split("\\], ");
        TIntObjectMap<TIntSet> mapSet = new TIntObjectHashMap<>(chunks.length+1);
        for(int i=0; i<chunks.length; i++) {
            String chunk = chunks[i];
            if(i==chunks.length-1) {
                chunk = chunk.substring(0, chunk.length()-1);
            }
            int equalsIndex = chunk.indexOf('=');
            int key = Integer.parseInt(chunk.substring(0, equalsIndex));
            String[] values = chunk.substring(equalsIndex+2).split(", ");
            TIntSet set = new TIntHashSet();
            for(String value : values) {
                if(!value.isEmpty()) {
                    set.add(Integer.parseInt(value));
                }
            }
            mapSet.put(key, set);

        }

        return mapSet;
    }

    static TIntSet parseSetLine(String line) {
        String[] values = line.substring(line.indexOf('[')+1, line.indexOf(']')).split(", ");
        TIntSet set = new TIntHashSet();
        for(String value : values) {
            if(!value.isEmpty()) {
                set.add(Integer.parseInt(value));
            }
        }
        return set;
    }
}
