package io.vntr.trace;

import io.vntr.IMiddleware;
import io.vntr.IMiddlewareAnalyzer;
import io.vntr.User;
import io.vntr.hermes.HermesInitUtils;
import io.vntr.hermes.HermesManager;
import io.vntr.hermes.HermesMiddleware;
import io.vntr.jabeja.JabejaInitUtils;
import io.vntr.jabeja.JabejaManager;
import io.vntr.jabeja.JabejaMiddleware;
import io.vntr.metis.MetisInitUtils;
import io.vntr.metis.MetisManager;
import io.vntr.metis.MetisMiddleware;
import io.vntr.spaja.SpajaInitUtils;
import io.vntr.spaja.SpajaManager;
import io.vntr.spaja.SpajaMiddleware;
import io.vntr.spar.SparInitUtils;
import io.vntr.spar.SparManager;
import io.vntr.spar.SparMiddleware;
import io.vntr.sparmes.SparmesInitUtils;
import io.vntr.sparmes.SparmesManager;
import io.vntr.sparmes.SparmesMiddleware;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class TraceRunner {
    public static final String SPAR_TYPE = "SPAR";
    public static final String JABEJA_TYPE = "JABEJA";
    public static final String HERMES_TYPE = "HERMES";
    public static final String SPAJA_TYPE = "SPAJA";
    public static final String SPARMES_TYPE = "SPARMES";
    public static final String METIS_TYPE = "METIS";
    private static final Set<String> allowedTypes = new HashSet<String>(Arrays.asList(JABEJA_TYPE, HERMES_TYPE, SPAR_TYPE, SPAJA_TYPE, SPARMES_TYPE, METIS_TYPE));

    private static final String overallFormatStr = "    %7s | %s | %-25s | Edge Cut = %7d | Replica Count = %7d";

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        prop.load(new FileInputStream("config.properties"));

        String inputFolder = prop.getProperty("input.folder");
        String outputFolder = prop.getProperty("output.folder");

        if(args.length < 2) {
            throw new IllegalArgumentException("Must have at least 2 arguments!");
        }

        String inputFile = inputFolder + File.separator + args[0];
        String type = args[1];
        String outputFile = outputFolder + File.separator + generateFilename(args);

        if(!allowedTypes.contains(type)) {
            throw new IllegalArgumentException("arg[1] must be one of " + allowedTypes);
        }

        System.out.println("Type: " + type);
        System.out.println("Input file: " + inputFile);
        System.out.println("Output file: " + outputFile);
        Trace trace = TraceUtils.getFullTraceFromFile(inputFile);

        IMiddlewareAnalyzer middleware;

        switch (type) {
            case JABEJA_TYPE:  middleware = initJabejaMiddleware (trace, args, prop); break;
            case HERMES_TYPE:  middleware = initHermesMiddleware (trace, args, prop); break;
            case SPAR_TYPE:    middleware = initSparMiddleware   (trace, args, prop); break;
            case SPAJA_TYPE:   middleware = initSpajaMiddleware  (trace, args, prop); break;
            case SPARMES_TYPE: middleware = initSparmesMiddleware(trace, args, prop); break;
            case METIS_TYPE:   middleware = initMetisMiddleware  (trace, args, prop); break;
            default: throw new RuntimeException("Must be one of " + allowedTypes);
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

            long startTime = System.nanoTime();

            for (int i = 0; i < trace.getActions().size(); i++) {
                TraceAction next = trace.getActions().get(i);
                log(middleware, pw, next.toString(), type, true, (i % 50) == 0);
                runAction(middleware, next);
            }

            long timeElapsed = System.nanoTime() - startTime;
            long seconds = timeElapsed / 1000000000;
            long millis  = (timeElapsed % 1000000000) / 1000000;

            System.out.println("Time elapsed: " + seconds + "." + millis + " seconds");

            log(middleware, pw, "N/A", type, true, true);

        }catch(Exception e) {
            throw e;
        }
        finally {
            if(pw != null) {
                pw.close();
            }
        }
    }

    static void runAction(IMiddleware middleware, TraceAction action) {
        switch (action.getTRACEAction()) {
            case ADD_USER:         middleware.addUser(new User(action.getVal1()));          break;
            case REMOVE_USER:      middleware.removeUser(action.getVal1());                 break;
            case BEFRIEND:         middleware.befriend(action.getVal1(), action.getVal2()); break;
            case UNFRIEND:         middleware.unfriend(action.getVal1(), action.getVal2()); break;
            case ADD_PARTITION:    middleware.addPartition(action.getVal1());               break;
            case REMOVE_PARTITION: middleware.removePartition(action.getVal1());            break;
            case DOWNTIME:         middleware.broadcastDowntime();                          break;
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

    static String generateFilename(String[] args) {
        long nanoTime = System.nanoTime();
        Date now = new Date();
        StringBuilder builder = new StringBuilder();
        for(int i=0; i<args.length; i++) {
            builder.append(sanitize(args[i]));
            builder.append("__");
        }
        builder.append(new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(now));
        builder.append("__");
        builder.append(nanoTime);
        builder.append(".txt");
        return builder.toString();
    }

    private static final String sanitize(String str) {
        return str.replaceAll("\\W", "-");
    }

    static JabejaMiddleware initJabejaMiddleware(Trace trace, String[] args, Properties prop) {
        if(args.length != 8) {
            throw new IllegalArgumentException("JABEJA requires 8 arguments");
        }
        float alpha = Float.parseFloat(args[2]);
        float initialT = Float.parseFloat(args[3]);
        float deltaT = Float.parseFloat(args[4]);
        float befriendInitialT = Float.parseFloat(args[5]);
        float befriendDeltaT = Float.parseFloat(args[6]);
        int k = Integer.parseInt(args[7]);
        JabejaManager jabejaManager = JabejaInitUtils.initGraph(alpha, initialT, deltaT, befriendInitialT, befriendDeltaT, k, trace.getPartitions(), trace.getFriendships());
        return new JabejaMiddleware(jabejaManager);
    }

    static HermesMiddleware initHermesMiddleware(Trace trace, String[] args, Properties prop) {
        HermesManager hermesManager;
        float gamma = Float.parseFloat(args[2]);

        if(args.length == 5) {
            float maxIterationToNumUsersRatio = Float.parseFloat(args[3]);
            int k = Integer.parseInt((args[4]));
            hermesManager = HermesInitUtils.initGraph(gamma, k, maxIterationToNumUsersRatio, trace.getPartitions(), trace.getFriendships());
        }
        else if(args.length == 4) {
            float maxIterationToNumUsersRatio = Float.parseFloat(args[3]);
            hermesManager = HermesInitUtils.initGraph(gamma, 3, maxIterationToNumUsersRatio, trace.getPartitions(), trace.getFriendships());
        } else {
            hermesManager = HermesInitUtils.initGraph(gamma, true, trace.getPartitions(), trace.getFriendships());
        }

        return new HermesMiddleware(hermesManager, hermesManager.getGamma());
    }

    static SparMiddleware initSparMiddleware(Trace trace, String[] args, Properties prop) {
        if(args.length != 3) {
            throw new IllegalArgumentException("SPAR requires 3 arguments");
        }
        int minNumReplicas = Integer.parseInt(args[2]);
        SparManager manager = SparInitUtils.initGraph(minNumReplicas, trace.getPartitions(), trace.getFriendships(), trace.getReplicas());
        return new SparMiddleware(manager);
    }

    static SpajaMiddleware initSpajaMiddleware(Trace trace, String[] args, Properties prop) {
        if(args.length != 7) {
            throw new IllegalArgumentException("SPAJA requires 7 arguments");
        }
        int minNumReplicas = Integer.parseInt(args[2]);
        float alpha = Float.parseFloat(args[3]);
        float initialT = Float.parseFloat(args[4]);
        float deltaT = Float.parseFloat(args[5]);
        int k = Integer.parseInt(args[6]);
        SpajaManager spajaManager = SpajaInitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, trace.getPartitions(), trace.getFriendships(), trace.getReplicas());
        return new SpajaMiddleware(spajaManager);
    }

    static SparmesMiddleware initSparmesMiddleware(Trace trace, String[] args, Properties prop) {
        if(args.length != 4) {
            throw new IllegalArgumentException("SPARMES requires 4 arguments");
        }
        int minNumReplicas = Integer.parseInt(args[2]);
        float gamma = Float.parseFloat(args[3]);
        SparmesManager sparmesManager = SparmesInitUtils.initGraph(minNumReplicas, gamma, true, trace.getPartitions(), trace.getFriendships(), trace.getReplicas());

        return new SparmesMiddleware(sparmesManager);
    }

    static MetisMiddleware initMetisMiddleware(Trace trace, String[] args, Properties prop) {
        String gpmetisLocation = prop.getProperty("gpmetis.location");
        String gpmetisTempdir = prop.getProperty("gpmetis.tempdir");
        MetisManager manager = MetisInitUtils.initGraph(trace.getPartitions(), trace.getFriendships(), gpmetisLocation, gpmetisTempdir);
        return new MetisMiddleware(manager);
    }
}
