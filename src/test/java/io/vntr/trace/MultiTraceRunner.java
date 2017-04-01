package io.vntr.trace;

import io.vntr.IMiddleware;
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

import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 * Created by robertlindquist on 10/20/16.
 */
public class MultiTraceRunner {
    public static void main(String[] args) throws Exception {
        String filename = "/Users/robertlindquist/Documents/enhanced_trace_251072845977020.txt";
        TraceWithReplicas trace = (TraceWithReplicas) TraceTestUtils.getFullTraceFromFile(filename);
        System.out.println("numUsers: " + trace.getFriendships().size());
        System.out.println("numPartitions: " + trace.getPids().size());
        System.out.println("First action: " + trace.getActions().get(0));

        SparManager sparManager = initSparManager(trace.getFriendships(), trace.getPartitions(), trace.getReplicas());
        SparMiddleware sparMiddleware = initSparMiddleware(sparManager);

        JabejaManager jabejaManager = initJabejaManager(trace.getFriendships(), trace.getPartitions());
        JabejaMiddleware jabejaMiddleware = initJabejaMiddleware(jabejaManager);

        HermesManager hermesManager = initHermesManager(trace.getFriendships(), trace.getPartitions());
        HermesMiddleware hermesMiddleware = initHermesMiddleware(hermesManager);

        SpajaManager spajaManager = initSpajaManager(trace.getFriendships(), trace.getPartitions(), trace.getReplicas());
        SpajaMiddleware spajaMiddleware = initSpajaMiddleware(spajaManager);

        System.out.println("SPAR   - Starting edge cut: " + sparMiddleware.getEdgeCut());
        System.out.println("Jabeja - Starting edge cut: " + jabejaMiddleware.getEdgeCut());
        System.out.println("Hermes - Starting edge cut: " + hermesMiddleware.getEdgeCut());
        System.out.println("Spaja  - Starting edge cut: " + spajaMiddleware.getEdgeCut());

        for(int i=0; i<trace.getActions().size(); i++) {
            if((i % 50) == 0) {
                System.out.println(i + " at " + new Date());
            }
            FullTraceAction action = trace.getActions().get(i);
            runAction(sparMiddleware,   action);
            runAction(jabejaMiddleware, action);
            runAction(hermesMiddleware, action);
            runAction(spajaMiddleware,  action);
        }

        System.out.println("SPAR   - Ending edge cut:   " + sparMiddleware.getEdgeCut());
        System.out.println("Jabeja - Ending edge cut:   " + jabejaMiddleware.getEdgeCut());
        System.out.println("Hermes - Ending edge cut:   " + hermesMiddleware.getEdgeCut());
        System.out.println("Spaja  - Ending edge cut:   " + spajaMiddleware.getEdgeCut());
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
        return JabejaTestUtils.initGraph(1.5f, 2f, 0.2f, 2f, 0.2f, 9, partitions, friendships);
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
