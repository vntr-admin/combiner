package io.vntr.trace;

import io.vntr.IMiddleware;
import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;
import io.vntr.hermes.HermesManager;
import io.vntr.hermes.HermesMiddleware;
import io.vntr.hermes.HermesTestUtils;
import io.vntr.jabeja.JabejaManager;
import io.vntr.jabeja.JabejaMiddleware;
import io.vntr.jabeja.JabejaTestUtils;
import io.vntr.spaja.SpajaManager;
import io.vntr.spaja.SpajaMiddleware;
import io.vntr.spaja.SpajaTestUtils;
import io.vntr.spar.SparManager;
import io.vntr.spar.SparMiddleware;
import io.vntr.spar.SparTestUtils;
import io.vntr.sparmes.SparmesManager;
import io.vntr.sparmes.SparmesMiddleware;
import io.vntr.sparmes.SparmesTestUtils;

import java.io.PrintWriter;
import java.util.*;

/**
 * Created by robertlindquist on 10/21/16.
 */
public class SingleTraceRunner {
    public static final String SPAR_TYPE = "SPAR";
    public static final String JABEJA_TYPE = "JABEJA";
    public static final String HERMES_TYPE = "HERMES";
    public static final String SPAJA_TYPE = "SPAJA";
    public static final String SPARMES_TYPE = "SPARMES";
    private static final Set<String> allowedTypes = new HashSet<String>(Arrays.asList(SPAR_TYPE, JABEJA_TYPE, HERMES_TYPE, SPAJA_TYPE));

    private static final String overallFormatStr = "\t%6s | %s | %-27s | Edge Cut = %8d | Replica Count = %8d";

    public static void main(String[] args) throws Exception {
        if(args.length < 4) {
            throw new IllegalArgumentException("Must have at least 4 arguments!");
        }
        String type = args[0];
        String inputFile = args[1];
        String outputFile = args[2];

        if(!allowedTypes.contains(type)) {
            throw new IllegalArgumentException("arg[0] must be one of " + allowedTypes);
        }

        TraceWithReplicas trace = (TraceWithReplicas) TraceUtils.getFullTraceFromFile(inputFile);

        List<String> thingsToLog = new LinkedList<String>();

        IMiddlewareAnalyzer middleware;

        if(SPAR_TYPE.equals(type)) {
            int minNumReplicas = Integer.parseInt(args[3]);
            SparManager sparManager = initSparManager(trace.getFriendships(), trace.getPartitions(), trace.getReplicas(), minNumReplicas);
            middleware = initSparMiddleware(sparManager);
        } else if(JABEJA_TYPE.equals(type)) {
            if(args.length != 7) {
                throw new IllegalArgumentException("JABEJA requires 7 arguments");
            }
            float alpha = Float.parseFloat(args[3]);
            float initialT = Float.parseFloat(args[4]);
            float deltaT = Float.parseFloat(args[5]);
            int k = Integer.parseInt(args[6]);
            JabejaManager jabejaManager = initJabejaManager(alpha, initialT, deltaT, k, trace.getFriendships(), trace.getPartitions());
            middleware = initJabejaMiddleware(jabejaManager);
        } else if(HERMES_TYPE.equals(type)) {
            float gamma = Float.parseFloat(args[3]);
            HermesManager hermesManager = initHermesManager(gamma, trace.getFriendships(), trace.getPartitions());
            middleware = initHermesMiddleware(hermesManager);
        } else if(SPAJA_TYPE.equals(type)) {
            if(args.length != 8) {
                throw new IllegalArgumentException("SPAJA requires 8 arguments");
            }
            int minNumReplicas = Integer.parseInt(args[3]);
            float alpha = Float.parseFloat(args[4]);
            float initialT = Float.parseFloat(args[5]);
            float deltaT = Float.parseFloat(args[6]);
            int k = Integer.parseInt(args[7]);
            SpajaManager spajaManager = initSpajaManager(minNumReplicas, alpha, initialT, deltaT, k, trace.getFriendships(), trace.getPartitions(), trace.getReplicas());
            middleware = initSpajaMiddleware(spajaManager);
        } else {
            throw new RuntimeException();
        }

        int numFriendships = 0;
        for(int uid : trace.getFriendships().keySet()) {
            numFriendships += trace.getFriendships().get(uid).size();
        }
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(outputFile);
            log(pw, type, true, true);
            log(pw, middleware.toString(), true, true);
            log(pw, "Num users:       " + trace.getFriendships().size(), true, true);
            log(pw, "Num friendships: " + numFriendships,                true, true);
            log(pw, "Num partitions:  " + trace.getPids().size(),        true, true);

            System.out.println("Start:");
            log(middleware, pw, "N/A", type, true, true);

            for (int i = 0; i < trace.getActions().size(); i++) {
                FullTraceAction next = trace.getActions().get(i);
                log(middleware, pw, next.toString(), type, true, (i % 50) == 0);
                runAction(middleware, next);
            }

            log(middleware, pw, "N/A", type, true, true);
            System.out.println("End:");
        } catch(Exception e) {
            throw e;
        } finally {
            if(pw != null) {
                pw.close();
            }
        }
    }

    static void runAction(IMiddleware middleware, FullTraceAction action) {
        switch (action.getTRACEAction()) {
            case ADD_USER:         middleware.addUser(new User(action.getVal1()));               break;
            case REMOVE_USER:      middleware.removeUser(action.getVal1());                      break;
            case BEFRIEND:         middleware.befriend(action.getVal1(), action.getVal2());      break;
            case UNFRIEND:         middleware.unfriend(action.getVal1(), action.getVal2());      break;
            case ADD_PARTITION:    middleware.addPartition(action.getVal1());                    break;
            case REMOVE_PARTITION: middleware.removePartition(action.getVal1());                 break;
            case DOWNTIME:         middleware.broadcastDowntime();                               break;
        }
    }

    private static void log(IMiddlewareAnalyzer middleware, PrintWriter pw, String next, String type, boolean flush, boolean echo) {
        String str = String.format(overallFormatStr, type, new Date().toString(), next, middleware.getEdgeCut(), middleware.getReplicationCount());
        log(pw, str, flush, echo);
    }

    private static void log(PrintWriter pw, String str, boolean flush, boolean echo) {
        pw.println(str);
        if(flush) {
            pw.flush();
        }
        if(echo) {
            System.out.println(str);
        }
    }

    private static SparManager initSparManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas, int minNumReplicas) {
        return SparTestUtils.initGraph(minNumReplicas, partitions, friendships, replicas);
    }

    private static SparMiddleware initSparMiddleware(SparManager manager) {
        return new SparMiddleware(manager);
    }

    private static HermesManager initHermesManager(float gamma, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) throws Exception {
        return HermesTestUtils.initGraph(gamma, true, partitions, friendships);
    }

    private static HermesMiddleware initHermesMiddleware(HermesManager manager) {
        return new HermesMiddleware(manager, 1.2f);
    }

    private static JabejaManager initJabejaManager(float alpha, float initialT, float deltaT, int k, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) throws Exception {
        return JabejaTestUtils.initGraph(alpha, initialT, deltaT, k, partitions, friendships);
    }

    private static JabejaMiddleware initJabejaMiddleware(JabejaManager manager) {
        return new JabejaMiddleware(manager);
    }

    private static SpajaManager initSpajaManager(int minNumReplicas, float alpha, float initialT, float deltaT, int k, Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SpajaTestUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, partitions, friendships, replicas);
    }

    private static SpajaMiddleware initSpajaMiddleware(SpajaManager manager) {
        return new SpajaMiddleware(manager);
    }

    private static SparmesManager initSparmesManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SparmesTestUtils.initGraph(2, 1.2f, true, partitions, friendships, replicas);
    }

    private static SparmesMiddleware initSparmesMiddleware(SparmesManager manager) {
        return new SparmesMiddleware(manager);
    }
}
