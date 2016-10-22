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
        if(args.length != 3) {
            throw new IllegalArgumentException("Must have 3 arguments!");
        }
        String type = args[0];
        String inputFile = args[1];
        String outputFile = args[2];

        if(!allowedTypes.contains(type)) {
            throw new IllegalArgumentException("arg[0] must be one of " + allowedTypes);
        }

        TraceWithReplicas trace = (TraceWithReplicas) TraceUtils.getFullTraceFromFile(inputFile);

        IMiddlewareAnalyzer middleware;

        if(SPAR_TYPE.equals(type)) {
            SparManager sparManager = initSparManager(trace.getFriendships(), trace.getPartitions(), trace.getReplicas());
            middleware = initSparMiddleware(sparManager);
        } else if(JABEJA_TYPE.equals(type)) {
            JabejaManager jabejaManager = initJabejaManager(trace.getFriendships(), trace.getPartitions());
            middleware = initJabejaMiddleware(jabejaManager);
        } else if(HERMES_TYPE.equals(type)) {
            HermesManager hermesManager = initHermesManager(trace.getFriendships(), trace.getPartitions());
            middleware = initHermesMiddleware(hermesManager);
        } else if(SPAJA_TYPE.equals(type)) {
            SpajaManager spajaManager = initSpajaManager(trace.getFriendships(), trace.getPartitions(), trace.getReplicas());
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
            log(pw, "Num users:       " + trace.getFriendships().size(), true, true);
            log(pw, "Num friendships: " + numFriendships,                true, true);
            log(pw, "Num partitions:  " + trace.getPids().size(),        true, true);

            System.out.println("Start:");
            log(middleware, pw, "N/A", type, true, true);

            for (int i = 0; i < trace.getActions().size(); i++) {
                FullTraceAction next = trace.getActions().get(i);
                log(middleware, pw, next.toString(), type, (i % 50) == 0, (i % 50) == 0);
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

    private static SparManager initSparManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SparTestUtils.initGraph(2, partitions, friendships, replicas);
    }

    private static SparMiddleware initSparMiddleware(SparManager manager) {
        return new SparMiddleware(manager);
    }

    private static HermesManager initHermesManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) throws Exception {
        return HermesTestUtils.initGraph(1.2f, true, partitions, friendships);
    }

    private static HermesMiddleware initHermesMiddleware(HermesManager manager) {
        return new HermesMiddleware(manager, 1.2f);
    }

    private static JabejaManager initJabejaManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions) throws Exception {
        return JabejaTestUtils.initGraph(1.5f, 2f, 0.2f, 9, partitions, friendships);
    }

    private static JabejaMiddleware initJabejaMiddleware(JabejaManager manager) {
        return new JabejaMiddleware(manager);
    }

    private static SpajaManager initSpajaManager(Map<Integer, Set<Integer>> friendships, Map<Integer, Set<Integer>> partitions, Map<Integer, Set<Integer>> replicas) {
        return SpajaTestUtils.initGraph(2, 1.5f, 2f, 0.2f, 9, partitions, friendships, replicas);
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
