package io.vntr.trace;

import io.vntr.middleware.*;
import io.vntr.User;
import io.vntr.manager.NoRepManager;
import io.vntr.utils.InitUtils;
import io.vntr.manager.RepManager;

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

    private static final Set<String> allowedTypes = new HashSet<>(Arrays.asList(HERMES_TYPE, HERMAR_TYPE, SPAR_TYPE, SPARMES_TYPE, METIS_TYPE, JABEJA_TYPE, JABAR_TYPE, SPAJA_TYPE, DUMMY_TYPE, RDUMMY_TYPE));

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final SimpleDateFormat filenameSdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

    public static void main(String[] args) throws Exception {

        Thread.sleep(5000);

        Properties props = new Properties();
        props.load(new FileInputStream(CONFIG_FILE));

        ParsedArgs parsedArgs = parseArgs(args, props);

        Trace trace = TraceUtils.getFullTraceFromFile(parsedArgs.getInputFile());
        int traceLengthLimit = parsedArgs.getNumActions() != null ? parsedArgs.getNumActions() : trace.getActions().size();

        IMiddlewareAnalyzer middleware = initMiddleware(parsedArgs, trace, props);

        PrintWriter pw = null;
        try {
            pw = new PrintWriter(parsedArgs.getOutputFile());

            log(pw, "#Arguments: " + parsedArgs.toString(), true, true);
            log(pw, HEADER, true, true);

            long startTime = System.nanoTime();

            for (int i = 0; i < traceLengthLimit; i++) {
                TraceAction next = trace.getActions().get(i);
                log(middleware, pw, next, parsedArgs.getType(), i, parsedArgs, true, (i % 50) == 0);
                runAction(middleware, next);
                if(parsedArgs.getValidityCheckProbability() != 0 && Math.random() < parsedArgs.getValidityCheckProbability()) {
                    middleware.checkValidity();
                }
            }

            long timeElapsedNanos = System.nanoTime() - startTime;
            System.out.println("Time elapsed: " + (timeElapsedNanos / BILLION) + "." + ((timeElapsedNanos % BILLION) / MILLION) + " seconds");

            log(middleware, pw, null, parsedArgs.getType(), -1, parsedArgs, true, true);

        } catch(Exception e) {
            throw e;
        }
        finally {
            if(pw != null) {
                pw.close();
            }
        }
    }

    static IMiddlewareAnalyzer initMiddleware(ParsedArgs parsedArgs, Trace trace, Properties props) {
        switch (parsedArgs.getType()) {
            case HERMES_TYPE:  return initHermesMiddleware       (trace, parsedArgs, props);
            case HERMAR_TYPE:  return initHermarMiddleware       (trace, parsedArgs, props);
            case SPAR_TYPE:    return initSparMiddleware         (trace, parsedArgs, props);
            case SPARMES_TYPE: return initSparmesMiddleware      (trace, parsedArgs, props);
            case METIS_TYPE:   return initMetisMiddleware        (trace, parsedArgs, props);
            case JABEJA_TYPE:  return initJabejaMiddleware       (trace, parsedArgs, props);
            case JABAR_TYPE:   return initJabarMiddleware        (trace, parsedArgs, props);
            case SPAJA_TYPE:   return initSpajaMiddleware        (trace, parsedArgs, props);
            case DUMMY_TYPE:   return initDummyMiddleware        (trace, parsedArgs, props);
            case RDUMMY_TYPE:  return initReplicaDummyMiddleware (trace, parsedArgs, props);
            default: throw new RuntimeException("args[1] must be one of " + allowedTypes);
        }
    }

    static ParsedArgs parseArgs(String[] args, Properties props) {

        String inputFolder = props.getProperty("input.folder");
        String outputFolder = props.getProperty("output.folder");

        if(args.length < 2) {
            throw new IllegalArgumentException("Must have at least 2 arguments!");
        }

        if(args.length % 2 == 1) {
            throw new IllegalArgumentException("Must have an even number of arguments!");
        }

        ParsedArgs parsedArgs = new ParsedArgs(args[1]);
        if(args[0].contains(File.separator)) {
            parsedArgs.setInputFile(args[0]);
        } else {
            parsedArgs.setInputFile(inputFolder + File.separator + args[0]);
        }

        parsedArgs.setOutputFile(outputFolder + File.separator + generateFilename(args));

        if(!allowedTypes.contains(parsedArgs.getType())) {
            throw new IllegalArgumentException("arg[1] must be one of " + allowedTypes);
        }

        for(int i=2; i<args.length; i += 2) {
            parsedArgs.setFlag(args[i], args[i+1]);
        }

        return parsedArgs;
    }

    static class ParsedArgs {
        private String type;
        private String inputFile;
        private String outputFile;
        private Integer numActions;
        private Float alpha;
        private Float initialT = 2f;
        private Float deltaT;
        private Integer jaK = 15;
        private Float gamma;
        private Integer hermesK = 3;
        private Integer minNumReplicas = 0;
        private Integer numRestarts = 10;
        private Integer maxIterations = 100;
        private double assortivityCheckProbability = 1;
        private double latencyCheckProbability = 1;
        private double validityCheckProbability = 0;
        private double logicalMigrationRatio = 0;


        public ParsedArgs(String type) {
            this.type = type;
            switch(type) {
                case JABEJA_TYPE:  alpha = 3f;    deltaT = 0.025f; numRestarts = 10; break;
                case JABAR_TYPE:   alpha = 3f;    deltaT = 0.025f; numRestarts = 1;  break;
                case HERMES_TYPE:  gamma = 1.15f;                                    break;
                case HERMAR_TYPE:  gamma = 1.15f;                                    break;
                case SPAJA_TYPE:   alpha = 1f;    deltaT = 0.5f;                     break;
                case SPARMES_TYPE: gamma = 1.01f;                                    break;
            }
        }

        public String getType() {
            return type;
        }

        public String getInputFile() {
            return inputFile;
        }

        public void setInputFile(String inputFile) {
            this.inputFile = inputFile;
        }

        public String getOutputFile() {
            return outputFile;
        }

        public void setOutputFile(String outputFile) {
            this.outputFile = outputFile;
        }

        public Integer getNumActions() {
            return numActions;
        }

        public void setNumActions(Integer numActions) {
            this.numActions = numActions;
        }

        public Float getAlpha() {
            return alpha;
        }

        public void setAlpha(Float alpha) {
            this.alpha = alpha;
        }

        public Float getInitialT() {
            return initialT;
        }

        public void setInitialT(Float initialT) {
            this.initialT = initialT;
        }

        public Float getDeltaT() {
            return deltaT;
        }

        public void setDeltaT(Float deltaT) {
            this.deltaT = deltaT;
        }

        public Integer getJaK() {
            return jaK;
        }

        public void setJaK(Integer jaK) {
            this.jaK = jaK;
        }

        public Float getGamma() {
            return gamma;
        }

        public void setGamma(Float gamma) {
            this.gamma = gamma;
        }

        public Integer getHermesK() {
            return hermesK;
        }

        public void setHermesK(Integer hermesK) {
            this.hermesK = hermesK;
        }

        public Integer getMinNumReplicas() {
            return minNumReplicas;
        }

        public void setMinNumReplicas(Integer minNumReplicas) {
            this.minNumReplicas = minNumReplicas;
        }

        public Integer getNumRestarts() {
            return numRestarts;
        }

        public void setNumRestarts(Integer numRestarts) {
            this.numRestarts = numRestarts;
        }

        public Integer getMaxIterations() {
            return maxIterations;
        }

        public void setMaxIterations(Integer maxIterations) {
            this.maxIterations = maxIterations;
        }

        public double getAssortivityCheckProbability() {
            return assortivityCheckProbability;
        }

        public void setAssortivityCheckProbability(double assortivityCheckProbability) {
            this.assortivityCheckProbability = assortivityCheckProbability;
        }

        public double getLatencyCheckProbability() {
            return latencyCheckProbability;
        }

        public void setLatencyCheckProbability(double latencyCheckProbability) {
            this.latencyCheckProbability = latencyCheckProbability;
        }

        public double getValidityCheckProbability() {
            return validityCheckProbability;
        }

        public void setValidityCheckProbability(double validityCheckProbability) {
            this.validityCheckProbability = validityCheckProbability;
        }

        public double getLogicalMigrationRatio() {
            return logicalMigrationRatio;
        }

        public void setLogicalMigrationRatio(double logicalMigrationRatio) {
            this.logicalMigrationRatio = logicalMigrationRatio;
        }

        public static final String NUM_ACTIONS_FLAG = "-n";
        public static final String REPLICAS_FLAG = "-minReps";
        public static final String GAMMA_FLAG = "-gamma";
        public static final String ALPHA_FLAG = "-alpha";
        public static final String INITIAL_T_FLAG = "-initT";
        public static final String DELTA_T_FLAG = "-deltaT";
        public static final String NEIGHBORHOOD_FLAG = "-nbhd";
        public static final String MAX_MOVES_FLAG = "-maxMove";
        public static final String ITERATIONS_FLAG = "-maxIter";
        public static final String NUM_RESTARTS_FLAG = "-restarts";
        public static final String ASSORTIVITY_FLAG = "-assortivity";
        public static final String LATENCY_FLAG = "-delay";
        public static final String VALIDITY_FLAG = "-validity";
        public static final String LOGICAL_FLAG = "-logMig";


        public void setFlag(String flag, String rawValue) {
            double parsed = Double.parseDouble(rawValue);
            switch(flag) {
                case NUM_ACTIONS_FLAG:         setNumActions((int) parsed);             break;
                case REPLICAS_FLAG:            setMinNumReplicas((int) parsed);         break;
                case GAMMA_FLAG:               setGamma((float) parsed);                break;
                case ALPHA_FLAG:               setAlpha((float) parsed);                break;
                case INITIAL_T_FLAG:           setInitialT((float) parsed);             break;
                case DELTA_T_FLAG:             setDeltaT((float) parsed);               break;
                case NEIGHBORHOOD_FLAG:        setJaK((int) parsed);                    break;
                case MAX_MOVES_FLAG:           setHermesK((int) parsed);                break;
                case NUM_RESTARTS_FLAG:        setNumRestarts((int) parsed);            break;
                case ASSORTIVITY_FLAG:         setAssortivityCheckProbability(parsed);  break;
                case LATENCY_FLAG:             setLatencyCheckProbability(parsed);      break;
                case VALIDITY_FLAG:            setValidityCheckProbability(parsed);     break;
                case LOGICAL_FLAG:             setLogicalMigrationRatio(parsed);        break;
                case ITERATIONS_FLAG:          setMaxIterations((int) parsed);          break;
                default: throw new RuntimeException(flag + " is not a valid flag");
            }
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();

            //General args
            builder.append(inputFile).append(' ').append(type)
                    .append(" assortivity=").append(assortivityCheckProbability)
                    .append(" latency=").append(latencyCheckProbability)
                    .append(" validity=").append(validityCheckProbability)
                    .append(" logicalRatio=").append(logicalMigrationRatio);

            if(numActions != null) {
                builder.append(" numActions=").append(numActions);
            }

            //type-specific args
            if(SPAR_TYPE.equals(type) || SPARMES_TYPE.equals(type) || SPAJA_TYPE.equals(type) || RDUMMY_TYPE.equals(type)) {
                builder.append(" minReplicas=").append(minNumReplicas);
            }
            if(JABEJA_TYPE.equals(type) || JABAR_TYPE.equals(type) || SPAJA_TYPE.equals(type)) {
                builder.append(" alpha=").append(alpha).append(" initialT=").append(initialT).append(" deltaT=").append(deltaT).append(" k=").append(jaK);
            }
            if(HERMES_TYPE.equals(type) || HERMAR_TYPE.equals(type) || SPARMES_TYPE.equals(type)) {
                builder.append(" gamma=").append(gamma);
            }
            if(HERMES_TYPE.equals(type) || HERMAR_TYPE.equals(type)) {
                builder.append(" k=").append(hermesK);
            }
            if(JABEJA_TYPE.equals(type)) {
                builder.append(" restarts=").append(numRestarts);
            }

            if(numActions != null) {
                builder.append(" numActions=").append(numActions);
            }

            return builder.toString();
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

    private static void log(IMiddlewareAnalyzer middleware, PrintWriter pw, TraceAction next, String type, int i, ParsedArgs parsedArgs, boolean flush, boolean echo) {
        int numU = middleware.getNumberOfUsers();
        int numF = middleware.getNumberOfFriendships();
        int numP = middleware.getNumberOfPartitions();
        int cut  = middleware.getEdgeCut();
        int reps = middleware.getReplicationCount();

        double asrt;
        double asrtProb = parsedArgs.getAssortivityCheckProbability();
        if(asrtProb == 0) {
            asrt = -99D;
        } else if(asrtProb == 1) {
            asrt = middleware.calculateAssortivity();
        } else {
            asrt = Math.random() < asrtProb ? middleware.calculateAssortivity() : -99D;
        }

        long tally = middleware.getMigrationTally();

        double delay;
        double delayProb = parsedArgs.getLatencyCheckProbability();
        if(delayProb == 0) {
            delay = -99D;
        } else if(delayProb == 1) {
            delay = middleware.calculateExpectedQueryDelay();
        } else {
            delay = Math.random() < delayProb ? middleware.calculateExpectedQueryDelay() : -99D;
        }

        String nextAction = next != null ? next.toAbbreviatedString() : "END";

        String tableStr = formatTable(i, type, new Date(), nextAction, numP, numU, numF, asrt, cut, reps, tally, delay);

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
                        .append(System.nanoTime())
                        .append(".txt");

        return builder.toString();
    }

    private static String sanitize(String str) {
        return str.replaceAll("\\W", "-");
    }

    static JabejaMiddleware initJabejaMiddleware(Trace trace, ParsedArgs parsedArgs, Properties props) {
        NoRepManager noRepManager =
                InitUtils.initNoRepManager(
                        parsedArgs.getLogicalMigrationRatio(),
                        true,
                        trace.getPartitions(),
                        trace.getFriendships());

        return new JabejaMiddleware(parsedArgs.getAlpha(),
                parsedArgs.getInitialT(),
                parsedArgs.getDeltaT(),
                parsedArgs.getJaK(),
                parsedArgs.getNumRestarts(),
                noRepManager);
    }

    static JabarMiddleware initJabarMiddleware(Trace trace, ParsedArgs parsedArgs, Properties props) {

        NoRepManager noRepManager =
                InitUtils.initNoRepManager(
                        parsedArgs.getLogicalMigrationRatio(),
                        true,
                        trace.getPartitions(),
                        trace.getFriendships());

        return new JabarMiddleware(parsedArgs.getAlpha(), parsedArgs.getInitialT(), parsedArgs.getDeltaT(), parsedArgs.getJaK(), noRepManager);
    }

    static HermesMiddleware initHermesMiddleware(Trace trace, ParsedArgs parsedArgs, Properties prop) {
        NoRepManager noRepManager =
                InitUtils.initNoRepManager(
                        parsedArgs.getLogicalMigrationRatio(),
                        false,
                        trace.getPartitions(),
                        trace.getFriendships());

        return new HermesMiddleware(parsedArgs.getGamma(), parsedArgs.getHermesK(), parsedArgs.getMaxIterations(), noRepManager);
    }

    static HermarMiddleware initHermarMiddleware(Trace trace, ParsedArgs parsedArgs, Properties prop) {
        NoRepManager noRepManager =
                InitUtils.initNoRepManager(
                        parsedArgs.getLogicalMigrationRatio(),
                        false,
                        trace.getPartitions(),
                        trace.getFriendships());

        return new HermarMiddleware(parsedArgs.getGamma(), parsedArgs.getHermesK(), parsedArgs.getMaxIterations(), noRepManager);
    }

    static SparMiddleware initSparMiddleware(Trace trace, ParsedArgs parsedArgs, Properties props) {
        RepManager repManager = InitUtils.initRepManager(parsedArgs.getMinNumReplicas(), 0, trace.getPartitions(), trace.getFriendships(), trace.getReplicas());
        return new SparMiddleware(repManager);
    }

    static SparmesMiddleware initSparmesMiddleware(Trace trace, ParsedArgs parsedArgs, Properties props) {
        RepManager repManager = InitUtils.initRepManager(
               parsedArgs.getMinNumReplicas(),
               parsedArgs.getLogicalMigrationRatio(),
               trace.getPartitions(),
               trace.getFriendships(),
               trace.getReplicas());

        return new SparmesMiddleware(
                parsedArgs.getMinNumReplicas(),
                parsedArgs.getGamma(),
                parsedArgs.getHermesK(),
                parsedArgs.getMaxIterations(),
                repManager);
    }

    static SpajaMiddleware initSpajaMiddleware(Trace trace, ParsedArgs parsedArgs, Properties props) {
        RepManager repManager = InitUtils.initRepManager(
                parsedArgs.getMinNumReplicas(),
                parsedArgs.getLogicalMigrationRatio(),
                trace.getPartitions(),
                trace.getFriendships(),
                trace.getReplicas()
        );

        return new SpajaMiddleware(parsedArgs.getMinNumReplicas(),
                parsedArgs.getAlpha(),
                parsedArgs.getInitialT(),
                parsedArgs.getDeltaT(),
                parsedArgs.getJaK(),
                repManager);
    }

    static MetisMiddleware initMetisMiddleware(Trace trace, ParsedArgs parsedArgs, Properties prop) {
        String gpmetisLocation = prop.getProperty("gpmetis.location");
        String gpmetisTempdir = prop.getProperty("gpmetis.tempdir");
        NoRepManager noRepManager = InitUtils.initNoRepManager(parsedArgs.getLogicalMigrationRatio(), false, trace.getPartitions(), trace.getFriendships());
        return new MetisMiddleware(gpmetisLocation, gpmetisTempdir, noRepManager);
    }

    static DummyMiddleware initDummyMiddleware(Trace trace, ParsedArgs parsedArgs, Properties prop) {
        NoRepManager noRepManager = InitUtils.initNoRepManager(parsedArgs.getLogicalMigrationRatio(), false, trace.getPartitions(), trace.getFriendships());
        return new DummyMiddleware(noRepManager);
    }

    static ReplicaDummyMiddleware initReplicaDummyMiddleware(Trace trace, ParsedArgs parsedArgs, Properties prop) {
        RepManager repManager = InitUtils.initRepManager(
                parsedArgs.getMinNumReplicas(),
                parsedArgs.getLogicalMigrationRatio(),
                trace.getPartitions(),
                trace.getFriendships(),
                trace.getReplicas()
        );

        return new ReplicaDummyMiddleware(repManager);
    }

    private static final String HEADER = "No       Type     Date                 Action          Ps   Nodes  Edges    Assort.  EdgeCut  Replicas  Moves     Delay";
    private static final String TABLE_FORMAT_STR = "%-8d %-8s %-20s %-15s %-4d %-6d %-8d %-8s %-8s %-9d %-8d  %-6d";

    static String formatTable(int i, String type, Date date, String nextAction, int numP, int numU, int numF, double asrt, int cut, int reps, long migrations, double latency) {
        String formattedDate = sdf.format(date);

        String asrtString = "" + asrt;
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
                numP,
                numU,
                numF,
                asrtString,
                cut,
                reps,
                migrations,
                (int) latency);

        return result;
    }

}
