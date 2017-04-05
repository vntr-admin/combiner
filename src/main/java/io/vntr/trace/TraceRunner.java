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
    private static final Set<String> allowedTypes = new HashSet<>(Arrays.asList(JABEJA_TYPE, HERMES_TYPE, SPAR_TYPE, SPAJA_TYPE, SPARMES_TYPE, METIS_TYPE));

    private static final String overallFormatStr = "    %7s | %s | %-25s | Edge Cut = %7d | Replica Count = %7d";

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        prop.load(new FileInputStream("config.properties"));

        String inputFolder = prop.getProperty("input.folder");
        String outputFolder = prop.getProperty("output.folder");

        if(args.length < 2) {
            throw new IllegalArgumentException("Must have at least 2 arguments!");
        }

        String inputFile;
        if(args[0].contains(File.separator)) {
            inputFile = args[0];
        } else {
            inputFile = inputFolder + File.separator + args[0];
        }

        String type = args[1];
        String outputFile = outputFolder + File.separator + generateFilename(args);

        if(!allowedTypes.contains(type)) {
            throw new IllegalArgumentException("arg[1] must be one of " + allowedTypes);
        }

        System.out.println("Type: " + type);
        System.out.println("Input file: " + inputFile);
        System.out.println("Output file: " + outputFile);
        Trace trace = TraceUtils.getFullTraceFromFile(inputFile);

        int traceLengthLimit;
        boolean hasLengthOverride = ( args.length >= 4 && "-n".equals(args[2]) );
        if(hasLengthOverride) {
            traceLengthLimit = Integer.parseInt(args[3]);
        } else {
            traceLengthLimit = trace.getActions().size();
        }

        String[] middlewareArgs = new String[hasLengthOverride ? args.length - 4 : args.length - 2];
        System.arraycopy(args, hasLengthOverride ? 4 : 2, middlewareArgs, 0, middlewareArgs.length);

        IMiddlewareAnalyzer middleware;

        switch (type) {
            case JABEJA_TYPE:  middleware = initJabejaMiddleware (trace, middlewareArgs, prop); break;
            case HERMES_TYPE:  middleware = initHermesMiddleware (trace, middlewareArgs, prop); break;
            case SPAR_TYPE:    middleware = initSparMiddleware   (trace, middlewareArgs, prop); break;
            case SPAJA_TYPE:   middleware = initSpajaMiddleware  (trace, middlewareArgs, prop); break;
            case SPARMES_TYPE: middleware = initSparmesMiddleware(trace, middlewareArgs, prop); break;
            case METIS_TYPE:   middleware = initMetisMiddleware  (trace, middlewareArgs, prop); break;
            default: throw new RuntimeException("Must be one of " + allowedTypes);
        }

        PrintWriter pw = null;
        try {
            pw = new PrintWriter(outputFile);

            StringBuilder builder = new StringBuilder();
            for(int i=0; i<args.length; i++) {
                builder.append(args[i]);
                if(i< args.length-1) {
                    builder.append(' ');
                }
            }
            log(pw, "Arguments: " + builder.toString(), true, true);
            log(pw, HEADER, true, true);
            log(middleware, pw, null, type, -1, true, true);

            long startTime = System.nanoTime();

            for (int i = 0; i < traceLengthLimit; i++) {
                TraceAction next = trace.getActions().get(i);
                log(middleware, pw, next, type, i, true, (i % 50) == 0);
                runAction(middleware, next);
            }

            long timeElapsed = System.nanoTime() - startTime;
            long seconds = timeElapsed / 1000000000;
            long millis  = (timeElapsed % 1000000000) / 1000000;

            System.out.println("Time elapsed: " + seconds + "." + millis + " seconds");

            log(middleware, pw, null, type, -1, true, true);

        }catch(Exception e) {
            throw e;
        }
        finally {
            if(pw != null) {
                pw.close();
            }
        }
    }

    static ParsedArgs parseArgs(String[] args, Properties props) {

        ParsedArgs parsedArgs = new ParsedArgs();

        String inputFolder = props.getProperty("input.folder");
        String outputFolder = props.getProperty("output.folder");

        if(args.length < 2) {
            throw new IllegalArgumentException("Must have at least 2 arguments!");
        }

        if(args.length % 2 == 1) {
            throw new IllegalArgumentException("Must have an even number of arguments!");
        }

        if(args[0].contains(File.separator)) {
            parsedArgs.setInputFile(args[0]);
        } else {
            parsedArgs.setInputFile(inputFolder + File.separator + args[0]);
        }

        parsedArgs.setType(args[1]);
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
        private Float initialT;
        private Float deltaT;
        private Float befriendInitialT;
        private Float befriendDeltaT;
        private Integer jaK;
        private Float gamma;
        private Float iterationCutoffRatio;
        private Integer hermesK;
        private Integer minNumReplicas;

        public ParsedArgs() {
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
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

        public Float getBefriendInitialT() {
            return befriendInitialT;
        }

        public void setBefriendInitialT(Float befriendInitialT) {
            this.befriendInitialT = befriendInitialT;
        }

        public Float getBefriendDeltaT() {
            return befriendDeltaT;
        }

        public void setBefriendDeltaT(Float befriendDeltaT) {
            this.befriendDeltaT = befriendDeltaT;
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

        public Float getIterationCutoffRatio() {
            return iterationCutoffRatio;
        }

        public void setIterationCutoffRatio(Float iterationCutoffRatio) {
            this.iterationCutoffRatio = iterationCutoffRatio;
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

        public static final String REPLICAS_FLAG = "-minReps";
        public static final String GAMMA_FLAG = "-gamma";
        public static final String ALPHA_FLAG = "-alpha";
        public static final String INITIAL_T_FLAG = "-initT";
        public static final String DELTA_T_FLAG = "-deltaT";
        public static final String NEIGHBORHOOD_FLAG = "-nbhd";
        public static final String MAX_MOVES_FLAG = "-maxMove";
        public static final String CUTOFF_FLAG = "-cutoff";
        public static final String INITIAL_T_BEFRIEND_FLAG = "-initTfriend";
        public static final String DELTA_T_BEFRIEND_FLAG = "-deltaTfriend";

        public void setFlag(String flag, String rawValue) {
            switch(flag) {
                case REPLICAS_FLAG:            setMinNumReplicas(Integer.parseInt(rawValue));       break;
                case GAMMA_FLAG:               setGamma(Float.parseFloat(rawValue));                break;
                case ALPHA_FLAG:               setAlpha(Float.parseFloat(rawValue));                break;
                case INITIAL_T_FLAG:           setInitialT(Float.parseFloat(rawValue));             break;
                case DELTA_T_FLAG:             setDeltaT(Float.parseFloat(rawValue));               break;
                case NEIGHBORHOOD_FLAG:        setJaK(Integer.parseInt(rawValue));                  break;
                case MAX_MOVES_FLAG:           setHermesK(Integer.parseInt(rawValue));              break;
                case CUTOFF_FLAG:              setIterationCutoffRatio(Float.parseFloat(rawValue)); break;
                case INITIAL_T_BEFRIEND_FLAG:  setBefriendInitialT(Float.parseFloat(rawValue));     break;
                case DELTA_T_BEFRIEND_FLAG:    setBefriendDeltaT(Float.parseFloat(rawValue));       break;
                default: throw new RuntimeException(flag + " is not a valid ");
            }
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

    private static void log(IMiddlewareAnalyzer middleware, PrintWriter pw, TraceAction next, String type, int i, boolean flush, boolean echo) {
        int numU = middleware.getNumberOfUsers();
        int numF = middleware.getNumberOfFriendships();
        int numP = middleware.getNumberOfPartitions();
        int cut  = middleware.getEdgeCut();
        int reps = middleware.getReplicationCount();
        double asrt = middleware.calculateAssortivity();
        String nextAction = next != null ? next.toAbbreviatedString() : "N/A";
        String compressedStr = formatCompressed(type, new Date(), nextAction, numP, numU, numF, asrt, cut, reps);
        String tableStr = formatTable(i, type, new Date(), nextAction, numP, numU, numF, asrt, cut, reps, middleware.getMigrationTally(), middleware.calculateExpectedQueryDelay());
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

    private static String sanitize(String str) {
        return str.replaceAll("\\W", "-");
    }

    static JabejaMiddleware initJabejaMiddleware(Trace trace, String[] args, Properties prop) {
        if(args.length != 6) {
            throw new IllegalArgumentException("JABEJA requires 6 arguments");
        }
        float alpha = Float.parseFloat(args[0]);
        float initialT = Float.parseFloat(args[1]);
        float deltaT = Float.parseFloat(args[2]);
        float befriendInitialT = Float.parseFloat(args[3]);
        float befriendDeltaT = Float.parseFloat(args[4]);
        int k = Integer.parseInt(args[5]);
        JabejaManager jabejaManager = JabejaInitUtils.initGraph(alpha, initialT, deltaT, befriendInitialT, befriendDeltaT, k, trace.getPartitions(), trace.getFriendships());
        return new JabejaMiddleware(jabejaManager);
    }

    static HermesMiddleware initHermesMiddleware(Trace trace, String[] args, Properties prop) {
        if(args.length > 3 || args.length < 1) {
            throw new IllegalArgumentException("HERMES requires 1-3 arguments");
        }

        HermesManager hermesManager;
        float gamma = Float.parseFloat(args[0]);

        if(args.length == 3) {
            float maxIterationToNumUsersRatio = Float.parseFloat(args[1]);
            int k = Integer.parseInt((args[2]));
            hermesManager = HermesInitUtils.initGraph(gamma, k, maxIterationToNumUsersRatio, trace.getPartitions(), trace.getFriendships());
        }
        else if(args.length == 2) {
            float maxIterationToNumUsersRatio = Float.parseFloat(args[1]);
            hermesManager = HermesInitUtils.initGraph(gamma, 3, maxIterationToNumUsersRatio, trace.getPartitions(), trace.getFriendships());
        } else {
            hermesManager = HermesInitUtils.initGraph(gamma, true, trace.getPartitions(), trace.getFriendships());
        }

        return new HermesMiddleware(hermesManager, hermesManager.getGamma());
    }

    static SparMiddleware initSparMiddleware(Trace trace, String[] args, Properties prop) {
        if(args.length != 1) {
            throw new IllegalArgumentException("SPAR requires 1 argument");
        }
        int minNumReplicas = Integer.parseInt(args[0]);
        SparManager manager = SparInitUtils.initGraph(minNumReplicas, trace.getPartitions(), trace.getFriendships(), trace.getReplicas());
        return new SparMiddleware(manager);
    }

    static SpajaMiddleware initSpajaMiddleware(Trace trace, String[] args, Properties prop) {
        if(args.length != 5) {
            throw new IllegalArgumentException("SPAJA requires 5 arguments");
        }
        int minNumReplicas = Integer.parseInt(args[0]);
        float alpha = Float.parseFloat(args[1]);
        float initialT = Float.parseFloat(args[2]);
        float deltaT = Float.parseFloat(args[3]);
        int k = Integer.parseInt(args[4]);
        SpajaManager spajaManager = SpajaInitUtils.initGraph(minNumReplicas, alpha, initialT, deltaT, k, trace.getPartitions(), trace.getFriendships(), trace.getReplicas());
        return new SpajaMiddleware(spajaManager);
    }

    static SparmesMiddleware initSparmesMiddleware(Trace trace, String[] args, Properties prop) {
        if(args.length != 2) {
            throw new IllegalArgumentException("SPARMES requires 2 arguments");
        }
        int minNumReplicas = Integer.parseInt(args[0]);
        float gamma = Float.parseFloat(args[1]);
        SparmesManager sparmesManager = SparmesInitUtils.initGraph(minNumReplicas, gamma, true, trace.getPartitions(), trace.getFriendships(), trace.getReplicas());

        return new SparmesMiddleware(sparmesManager);
    }

    static MetisMiddleware initMetisMiddleware(Trace trace, String[] args, Properties prop) {
        String gpmetisLocation = prop.getProperty("gpmetis.location");
        String gpmetisTempdir = prop.getProperty("gpmetis.tempdir");
        MetisManager manager = MetisInitUtils.initGraph(trace.getPartitions(), trace.getFriendships(), gpmetisLocation, gpmetisTempdir);
        return new MetisMiddleware(manager);
    }

    private static final String compressedFormatStr = "#%7s | %19s | %-14s | P=%-3d | U=%-5d | F=%-7d | ASRT=%-6s | Cut=%-7s | Reps=%-7s";

    static String formatCompressed(String type, Date date, String nextAction, int numP, int numU, int numF, double asrt, int cut, int reps) {

        String formattedDate = sdf.format(date);

        String asrtString = "" + asrt;
        if(asrtString.startsWith("0")) {
            asrtString = asrtString.substring(1);
        }
        else if(asrtString.startsWith("-0")) {
            asrtString = "-" + asrtString.substring(2);
        }
        if(asrtString.length() > 6) {
            asrtString = asrtString.substring(0,6);
        }

        String result = String.format(compressedFormatStr,
                type,
                formattedDate,
                nextAction,
                numP,
                numU,
                numF,
                asrtString,
                cut,
                reps);

        return result;
    }

    private static final String HEADER = "No       Type     Date                 Action          Ps   Nodes  Edges    Assort.  EdgeCut  Replicas  Moves     Delay";
    private static final String TABLE_FORMAT_STR = "%-8d %-8s %-20s %-15s %-4d %-6d %-8d %-8s %-8s %-9d %-8d  %-7d";

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
