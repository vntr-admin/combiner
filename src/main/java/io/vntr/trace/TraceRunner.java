package io.vntr.trace;

import io.vntr.middleware.*;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.utils.InitUtils;
import io.vntr.manager.RepManager;

import java.io.FileInputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

import static io.vntr.trace.TraceAction.ACTION.*;

/**
 * Created by robertlindquist on 4/1/17.
 */
public class TraceRunner {
    public static final String SPAR_TYPE = "SPAR";
    public static final String HERMES_TYPE = "HERMES";
    public static final String HERMAR_TYPE = "HERMAR";
    public static final String SPARMES_TYPE = "SPARMES";
    public static final String METIS_TYPE = "METIS";
    public static final String JABEJA_TYPE = "JABEJA";
    public static final String JABAR_TYPE = "JABAR";
    public static final String SPAJA_TYPE = "SPAJA";
    public static final String DUMMY_TYPE = "DUMMY";
    public static final String RDUMMY_TYPE = "RDUMMY";

    public static final long MILLION = 1000000;
    public static final long BILLION = 1000000000;

    private static final String CONFIG_FILE = "config.properties";

    static final Set<String> allowedTypes = new HashSet<>(Arrays.asList(HERMES_TYPE, HERMAR_TYPE, SPAR_TYPE, SPARMES_TYPE, METIS_TYPE, JABEJA_TYPE, JABAR_TYPE, SPAJA_TYPE, DUMMY_TYPE, RDUMMY_TYPE));

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final SimpleDateFormat filenameSdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

    public static void main(String[] args) throws Exception {

        Thread.sleep(5000);

        Properties props = new Properties();
        props.load(new FileInputStream(CONFIG_FILE));

        TraceArgs traceArgs = TraceArgs.parseArgs(args, props);

        Trace trace = TraceUtils.getFullTraceFromFile(traceArgs.getInputFile());
        int traceLengthLimit = traceArgs.getNumActions() != null ? traceArgs.getNumActions() : trace.getActions().size();
        Recorder recorder = new Recorder(traceLengthLimit);

        IMiddlewareAnalyzer middleware = initMiddleware(traceArgs, trace, props);

        PrintWriter pw = null;
        PrintWriter csvPw = null;
        try {
            pw = new PrintWriter(traceArgs.getOutputFile());
            if(traceArgs.isExportCSV()) {
                csvPw = new PrintWriter(traceArgs.getCsvFile());
                log(csvPw, CSV_HEADER, true, true);
            }

            log(pw, "#Arguments: " + traceArgs.toString(), true, true);
            log(pw, HEADER, true, true);

            long startTime = System.nanoTime();

            int preRep = middleware.getReplicationCount();
            int preCut = middleware.getEdgeCut();
            for (int i = 0; i < traceLengthLimit; i++) {
                TraceAction next = trace.getActions().get(i);

                Status status = Status.initStatus(middleware, traceArgs);
                log(status, pw, next, traceArgs.getType(), i, false, (i % 50) == 0);
                if(traceArgs.isExportCSV()) {
                    log(csvPw, formatCsv(i, status), false, false);
                }

                runAction(middleware, next);
                if(traceArgs.getValidityCheckProbability() != 0 && Math.random() < traceArgs.getValidityCheckProbability()) {
                    middleware.checkValidity();
                }
                int postCut = middleware.getEdgeCut();
                int postRep = middleware.getReplicationCount();
                recorder.getDeltaEdgeCuts().put(next.getAction(), recorder.getDeltaEdgeCuts().get(next.getAction()) + postCut - preCut);
                recorder.getDeltaReps().put(next.getAction(), recorder.getDeltaReps().get(next.getAction()) + postRep - preRep);
                preCut = postCut;
                preRep = postRep;
            }

            long timeElapsedNanos = System.nanoTime() - startTime;
            System.out.println("Time elapsed: " + (timeElapsedNanos / BILLION) + "." + ((timeElapsedNanos % BILLION) / MILLION) + " seconds");

            Status status = Status.initStatus(middleware, traceArgs);
            log(status, pw, null, traceArgs.getType(), traceLengthLimit, true, true);
            if(traceArgs.isExportCSV()) {
                log(csvPw, formatCsv(traceLengthLimit, status), false, false);
            }

            log(recorder, pw);

        } catch(Exception e) {
            throw e;
        }
        finally {
            if(pw != null) {
                pw.close();
            }
            if(csvPw != null) {
                csvPw.close();
            }
        }
    }

    static IMiddlewareAnalyzer initMiddleware(TraceArgs traceArgs, Trace trace, Properties props) {
        switch (traceArgs.getType()) {
            case HERMES_TYPE:  return initHermesMiddleware       (trace, traceArgs, props);
            case HERMAR_TYPE:  return initHermarMiddleware       (trace, traceArgs, props);
            case SPAR_TYPE:    return initSparMiddleware         (trace, traceArgs, props);
            case SPARMES_TYPE: return initSparmesMiddleware      (trace, traceArgs, props);
            case METIS_TYPE:   return initMetisMiddleware        (trace, traceArgs, props);
            case JABEJA_TYPE:  return initJabejaMiddleware       (trace, traceArgs, props);
            case JABAR_TYPE:   return initJabarMiddleware        (trace, traceArgs, props);
            case SPAJA_TYPE:   return initSpajaMiddleware        (trace, traceArgs, props);
            case DUMMY_TYPE:   return initDummyMiddleware        (trace, traceArgs, props);
            case RDUMMY_TYPE:  return initReplicaDummyMiddleware (trace, traceArgs, props);
            default: throw new RuntimeException("args[1] must be one of " + allowedTypes);
        }
    }

    static class Recorder {
        private List<Integer> indexList;
        private List<Integer> numPartitionsList;
        private List<Integer> numUsersList;
        private List<Integer> numFriendshipsList;
        private List<Double>  assortivityList;
        private List<Integer> edgeCutList;
        private List<Integer> numReplicasList;
        private List<Long> numMovesList;
        private List<Double>  delayList;
        private Map<TraceAction.ACTION, Integer> deltaEdgeCuts;
        private Map<TraceAction.ACTION, Integer> deltaReps;

        public Recorder(int traceLength) {
            indexList         = new ArrayList<>(traceLength+1);
            numPartitionsList = new ArrayList<>(traceLength+1);
            numUsersList      = new ArrayList<>(traceLength+1);
            numFriendshipsList = new ArrayList<>(traceLength+1);
            assortivityList   = new ArrayList<>(traceLength+1);
            edgeCutList       = new ArrayList<>(traceLength+1);
            numReplicasList   = new ArrayList<>(traceLength+1);
            numMovesList      = new ArrayList<>(traceLength+1);
            delayList         = new ArrayList<>(traceLength+1);

            deltaEdgeCuts = new HashMap<>();
            deltaReps     = new HashMap<>();
            for(TraceAction.ACTION action : TraceAction.ACTION.values()) {
                deltaEdgeCuts.put(action, 0);
                deltaReps.put(action, 0);
            }
        }

        public List<Integer> getIndexList() {
            return indexList;
        }

        public List<Integer> getNumPartitionsList() {
            return numPartitionsList;
        }

        public List<Integer> getNumUsersList() {
            return numUsersList;
        }

        public List<Integer> getNumFriendshipsList() {
            return numFriendshipsList;
        }

        public List<Double> getAssortivityList() {
            return assortivityList;
        }

        public List<Integer> getEdgeCutList() {
            return edgeCutList;
        }

        public List<Integer> getNumReplicasList() {
            return numReplicasList;
        }

        public List<Long> getNumMovesList() {
            return numMovesList;
        }

        public List<Double> getDelayList() {
            return delayList;
        }

        public Map<TraceAction.ACTION, Integer> getDeltaEdgeCuts() {
            return deltaEdgeCuts;
        }

        public Map<TraceAction.ACTION, Integer> getDeltaReps() {
            return deltaReps;
        }
    }

    static void runAction(IMiddleware middleware, TraceAction action) {
        switch (action.getAction()) {
            case ADD_USER:         middleware.addUser(new User(action.getVal1()));          break;
            case REMOVE_USER:      middleware.removeUser(action.getVal1());                 break;
            case BEFRIEND:         middleware.befriend(action.getVal1(), action.getVal2()); break;
            case UNFRIEND:         middleware.unfriend(action.getVal1(), action.getVal2()); break;
            case ADD_PARTITION:    middleware.addPartition(action.getVal1());               break;
            case REMOVE_PARTITION: middleware.removePartition(action.getVal1());            break;
            case DOWNTIME:         middleware.broadcastDowntime();                          break;
        }
    }

    private static final String actionFormatStr = "| %2s | %9d | %9d |";
    private static void log(Recorder recorder, PrintWriter pw) {
        String title  = "\nIMPACT OF ACTIONS ON CUTS/REPS\n";
        String stars  = "+----+-----------+-----------+";
        String header = "| AC | EDGE CUT  | REPLICAS  |";
        log(pw, title, false, true);
        log(pw, stars, false, true);
        log(pw, header,false, true);
        log(pw, stars, false, true);
        log(pw, formatActionLine(recorder, ADD_USER),        false, true);
        log(pw, formatActionLine(recorder, REMOVE_USER),     false, true);
        log(pw, formatActionLine(recorder, BEFRIEND),        false, true);
        log(pw, formatActionLine(recorder, UNFRIEND),        false, true);
        log(pw, formatActionLine(recorder, ADD_PARTITION),   false, true);
        log(pw, formatActionLine(recorder, REMOVE_PARTITION),false, true);
        log(pw, formatActionLine(recorder, DOWNTIME),        false, true);
        log(pw, stars, true, true);
    }

    private static String formatActionLine(Recorder recorder, TraceAction.ACTION action) {
        return String.format(actionFormatStr, action.getAbbreviation(), recorder.getDeltaEdgeCuts().get(action), recorder.getDeltaReps().get(action));
    }

    private static class Status {
        public final int numU;
        public final int numF;
        public final int numP;
        public final int cut ;
        public final int reps;
        public final long tally;
        public final double asrt;
        public final double delay;

        public Status(int numU, int numF, int numP, int cut, int reps, long tally, double asrt, double delay) {
            this.numU = numU;
            this.numF = numF;
            this.numP = numP;
            this.cut = cut;
            this.reps = reps;
            this.tally = tally;
            this.asrt = asrt;
            this.delay = delay;
        }

        public static Status initStatus(IMiddlewareAnalyzer middleware, TraceArgs traceArgs) {
            int numU = middleware.getNumberOfUsers();
            int numF = middleware.getNumberOfFriendships();
            int numP = middleware.getNumberOfPartitions();
            int cut  = middleware.getEdgeCut();
            int reps = middleware.getReplicationCount();

            double asrt;
            double asrtProb = traceArgs.getAssortivityCheckProbability();
            if(asrtProb == 0) {
                asrt = -99D;
            } else if(asrtProb == 1) {
                asrt = middleware.calculateAssortivity();
            } else {
                asrt = Math.random() < asrtProb ? middleware.calculateAssortivity() : -99D;
            }

            long tally = middleware.getMigrationTally();

            double delay;
            double delayProb = traceArgs.getLatencyCheckProbability();
            if(delayProb == 0) {
                delay = -99D;
            } else if(delayProb == 1) {
                delay = middleware.calculateExpectedQueryDelay();
            } else {
                delay = Math.random() < delayProb ? middleware.calculateExpectedQueryDelay() : -99D;
            }

            return new Status(numU, numF, numP, cut, reps, tally, asrt, delay);
        }
    }

    private static void log(Status status, PrintWriter pw, TraceAction next, String type, int i, boolean flush, boolean echo) {
        String nextAction = next != null ? next.toAbbreviatedString() : "END";
        String tableStr = formatTable(i, type, new Date(), nextAction, status);
        log(pw, tableStr, flush, echo);
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
        String cleanInputFilename = sanitize((args[0]));
        String type = args[1];

        StringBuilder builder =
                new StringBuilder()
                        .append(cleanInputFilename)
                        .append("__")
                        .append(type)
                        .append("__")
                        .append(filenameSdf.format(new Date()))
                        .append("__")
                        .append(System.nanoTime());

        return builder.toString();
    }

    private static String sanitize(String str) {
        return str.replaceAll("\\W", "-");
    }

    static JabejaMiddleware initJabejaMiddleware(Trace trace, TraceArgs traceArgs, Properties props) {
        NoRepManager noRepManager =
                InitUtils.initNoRepManager(
                        traceArgs.getLogicalMigrationRatio(),
                        true,
                        trace.getPartitions(),
                        trace.getFriendships());

        return new JabejaMiddleware(traceArgs.getAlpha(),
                traceArgs.getInitialT(),
                traceArgs.getDeltaT(),
                traceArgs.getJaK(),
                traceArgs.getNumRestarts(),
                noRepManager);
    }

    static JabarMiddleware initJabarMiddleware(Trace trace, TraceArgs traceArgs, Properties props) {

        NoRepManager noRepManager =
                InitUtils.initNoRepManager(
                        traceArgs.getLogicalMigrationRatio(),
                        true,
                        trace.getPartitions(),
                        trace.getFriendships());

        return new JabarMiddleware(traceArgs.getAlpha(), traceArgs.getInitialT(), traceArgs.getDeltaT(), traceArgs.getJaK(), noRepManager);
    }

    static HermesMiddleware initHermesMiddleware(Trace trace, TraceArgs traceArgs, Properties prop) {
        NoRepManager noRepManager =
                InitUtils.initNoRepManager(
                        traceArgs.getLogicalMigrationRatio(),
                        false,
                        trace.getPartitions(),
                        trace.getFriendships());

        return new HermesMiddleware(traceArgs.getGamma(), traceArgs.getHermesK(), traceArgs.getMaxIterations(), noRepManager);
    }

    static HermarMiddleware initHermarMiddleware(Trace trace, TraceArgs traceArgs, Properties prop) {
        NoRepManager noRepManager =
                InitUtils.initNoRepManager(
                        traceArgs.getLogicalMigrationRatio(),
                        false,
                        trace.getPartitions(),
                        trace.getFriendships());

        return new HermarMiddleware(traceArgs.getGamma(), traceArgs.getHermesK(), traceArgs.getMaxIterations(), noRepManager);
    }

    static SparMiddleware initSparMiddleware(Trace trace, TraceArgs traceArgs, Properties props) {
        RepManager repManager = InitUtils.initRepManager(traceArgs.getMinNumReplicas(), 0, trace.getPartitions(), trace.getFriendships(), trace.getReplicas());
        return new SparMiddleware(repManager);
    }

    static SparmesMiddleware initSparmesMiddleware(Trace trace, TraceArgs traceArgs, Properties props) {
        RepManager repManager = InitUtils.initRepManager(
               traceArgs.getMinNumReplicas(),
               traceArgs.getLogicalMigrationRatio(),
               trace.getPartitions(),
               trace.getFriendships(),
               trace.getReplicas());

        return new SparmesMiddleware(
                traceArgs.getMinNumReplicas(),
                traceArgs.getGamma(),
                traceArgs.getHermesK(),
                traceArgs.getMaxIterations(),
                repManager);
    }

    static SpajaMiddleware initSpajaMiddleware(Trace trace, TraceArgs traceArgs, Properties props) {
        RepManager repManager = InitUtils.initRepManager(
                traceArgs.getMinNumReplicas(),
                traceArgs.getLogicalMigrationRatio(),
                trace.getPartitions(),
                trace.getFriendships(),
                trace.getReplicas()
        );

        return new SpajaMiddleware(traceArgs.getMinNumReplicas(),
                traceArgs.getAlpha(),
                traceArgs.getInitialT(),
                traceArgs.getDeltaT(),
                traceArgs.getJaK(),
                repManager);
    }

    static MetisMiddleware initMetisMiddleware(Trace trace, TraceArgs traceArgs, Properties prop) {
        String gpmetisLocation = prop.getProperty("gpmetis.location");
        String gpmetisTempdir = prop.getProperty("gpmetis.tempdir");
        NoRepManager noRepManager = InitUtils.initNoRepManager(traceArgs.getLogicalMigrationRatio(), false, trace.getPartitions(), trace.getFriendships());
        return new MetisMiddleware(gpmetisLocation, gpmetisTempdir, noRepManager);
    }

    static DummyMiddleware initDummyMiddleware(Trace trace, TraceArgs traceArgs, Properties prop) {
        NoRepManager noRepManager = InitUtils.initNoRepManager(traceArgs.getLogicalMigrationRatio(), false, trace.getPartitions(), trace.getFriendships());
        return new DummyMiddleware(noRepManager);
    }

    static ReplicaDummyMiddleware initReplicaDummyMiddleware(Trace trace, TraceArgs traceArgs, Properties prop) {
        RepManager repManager = InitUtils.initRepManager(
                traceArgs.getMinNumReplicas(),
                traceArgs.getLogicalMigrationRatio(),
                trace.getPartitions(),
                trace.getFriendships(),
                trace.getReplicas()
        );

        return new ReplicaDummyMiddleware(repManager);
    }

    private static final String CSV_HEADER = "index,numP,numU,numF,asrt,cut,reps,moves,delay";
    private static final String HEADER = "No       Type     Date                 Action          Ps   Nodes  Edges    Assort.  EdgeCut  Replicas  Moves     Delay";
    private static final String TABLE_FORMAT_STR = "%-8d %-8s %-20s %-15s %-4d %-6d %-8d %-8s %-8s %-9d %-8d  %-6d";

    static String formatCsv(int i, Status status) {
        StringBuilder builder = new StringBuilder();
        builder .append(i)           .append(',')
                .append(status.numP) .append(',')
                .append(status.numU) .append(',')
                .append(status.numF) .append(',')
                .append(status.asrt) .append(',')
                .append(status.cut)  .append(',')
                .append(status.reps) .append(',')
                .append(status.tally).append(',')
                .append(status.delay).append(',');

        return builder.toString();
    }

    static String formatTable(int i, String type, Date date, String nextAction, Status status) {
        String formattedDate = sdf.format(date);

        String asrtString = "" + status.asrt;
        if(asrtString.startsWith("0")) {
            asrtString = asrtString.substring(1);
        }
        else if(asrtString.startsWith("-0")) {
            asrtString = "-" + asrtString.substring(2);
        }
        if(asrtString.length() > 7) {
            asrtString = asrtString.substring(0,7);
        }

        String result = String.format(TABLE_FORMAT_STR,
                i,
                type,
                formattedDate,
                nextAction,
                status.numP,
                status.numU,
                status.numF,
                asrtString,
                status.cut,
                status.reps,
                status.tally,
                (int) status.delay);

        return result;
    }

}
