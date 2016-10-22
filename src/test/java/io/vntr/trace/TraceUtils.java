package io.vntr.trace;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class TraceUtils {

    //TODO: make this into Trace, not just the List of actions
    public static List<FullTraceAction> getTraceFromFile(String filename){
        List<FullTraceAction> list = new LinkedList<FullTraceAction>();
        Scanner scanner = null;

        try {
            scanner = new Scanner(new File(filename));
            while(scanner.hasNextLine()) {
                String line = scanner.nextLine();
                FullTraceAction fullTraceAction = FullTraceAction.fromString(line);
                list.add(fullTraceAction);
            }
        } catch(Exception e) {
        } finally {
            if(scanner != null) {
                scanner.close();
            }
        }
        return list;
    }

    //TODO: make this into Trace, not just the List of actions
    public static void writeTraceToFile(String filename, List<FullTraceAction> trace) throws Exception {
        PrintWriter pw = new PrintWriter(new File(filename));
        for(Iterator<FullTraceAction> iter = trace.iterator(); iter.hasNext(); ) {
            FullTraceAction fullTraceAction = iter.next();
            pw.println(fullTraceAction.toString());
            if(Math.random() > .99) {
                pw.flush();
            }
        }
        pw.flush();
        pw.close();
    }

    public static Trace getFullTraceFromFile(String filename) {
        Trace trace = null;
        List<FullTraceAction> actions = new LinkedList<FullTraceAction>();
        Map<Integer, Set<Integer>> friendships = null;
        Set<Integer> pids = null;
        Map<Integer, Set<Integer>> partitions = null;
        Map<Integer, Set<Integer>> replicas = null;

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
                    if(replicas != null) {
                        TraceWithReplicas traceWithReplicas = new TraceWithReplicas();
                        traceWithReplicas.setReplicas(replicas);
                        traceWithReplicas.setFriendships(friendships);
                        traceWithReplicas.setPartitions(partitions);
                        trace = traceWithReplicas;
                    }
                    else if(partitions != null) {
                        TraceWithPartitions traceWithPartitions = new TraceWithPartitions();
                        traceWithPartitions.setPartitions(partitions);
                        traceWithPartitions.setFriendships(friendships);
                        trace = traceWithPartitions;
                    }
                    else {
                        trace = new Trace();
                        trace.setFriendships(friendships);
                    }
                    trace.setActions(actions);

                    FullTraceAction action = FullTraceAction.fromString(line.substring(line.indexOf(':')+1).trim());
                    actions.add(action);

                } else if(line.startsWith("A")) {
                    FullTraceAction action = FullTraceAction.fromString(line.substring(line.indexOf(':')+1).trim());
                    actions.add(action);
                }
            }
        } catch(Exception e) {
        } finally {
            if(scanner != null) {
                scanner.close();
            }
        }
        return trace;
    }

    static Map<Integer, Set<Integer>> parseMapSetLine(String line) {
        String tempLine = line.substring(1, line.length()-1);
        String[] chunks = tempLine.split("\\], ");
        Map<Integer, Set<Integer>> mapSet = new HashMap<Integer, Set<Integer>>();
        for(int i=0; i<chunks.length; i++) {
            String chunk = chunks[i];
            if(i==chunks.length-1) {
                chunk = chunk.substring(0, chunk.length()-1);
            }
            int equalsIndex = chunk.indexOf('=');
            int key = Integer.parseInt(chunk.substring(0, equalsIndex));
            String[] values = chunk.substring(equalsIndex+2).split(", ");
            Set<Integer> set = new HashSet<Integer>();
            for(String value : values) {
                if(!value.isEmpty()) {
                    set.add(Integer.parseInt(value));
                }
            }
            mapSet.put(key, set);

        }

        return mapSet;
    }

    static Set<Integer> parseSetLine(String line) {
        String[] values = line.substring(line.indexOf('[')+1, line.indexOf(']')).split(", ");
        Set<Integer> set = new HashSet<Integer>();
        for(String value : values) {
            if(!value.isEmpty()) {
                set.add(Integer.parseInt(value));
            }
        }
        return set;
    }


    public static void writeTraceToFile(String filename, Trace trace) throws Exception {
        PrintWriter pw = new PrintWriter(new File(filename));
        pw.println("F: " + trace.getFriendships());
        pw.flush();
        pw.println("PIDS: " + trace.getPids());
        if(trace instanceof TraceWithPartitions) {
            pw.println("P: " + ((TraceWithPartitions) trace).getPartitions());
            pw.flush();
        }
        if(trace instanceof TraceWithReplicas) {
            pw.println("R: " + ((TraceWithReplicas) trace).getReplicas());
            pw.flush();
        }
        double log10 = Math.log10(trace.getActions().size());
        int log10Rounded = (int) Math.ceil(log10);
        String formatStr = "A%-" + log10Rounded + "d: %s";
        for(int i=0; i<trace.getActions().size(); i++) {
            pw.println(String.format(formatStr, i, trace.getActions().get(i)));
        }
        pw.flush();
        pw.close();
    }
}
