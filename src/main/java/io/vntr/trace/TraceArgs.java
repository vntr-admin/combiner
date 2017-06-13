package io.vntr.trace;

import java.io.File;
import java.util.Properties;

import static io.vntr.trace.TraceRunner.*;

/**
 * Created by robertlindquist on 5/5/17.
 */
public class TraceArgs {
    private String type;
    private String inputFile;
    private String outputFile;
    private String csvFile;
    private Integer numActions;
    private Float alpha = 3f;
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
    private boolean exportCSV = true;


    public TraceArgs(String type) {
        this.type = type;
        switch(type) {
            case JABEJA_TYPE:  deltaT = 0.025f; numRestarts = 3;  break;
            case J2_TYPE:
            case JABAR_TYPE:   deltaT = 0.025f; numRestarts = 1;  break;
            case HERMES_TYPE:  gamma = 1.1f;                      break;
            case HERMAR_TYPE:  gamma = 1.1f;                      break;
            case SPAJA_TYPE:   deltaT = 0.5f;                     break;
            case SPARMES_TYPE: gamma = 1.05f;                     break;
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

    public String getCsvFile() {
        return csvFile;
    }

    public void setCsvFile(String csvFile) {
        this.csvFile = csvFile;
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

    public boolean isExportCSV() {
        return exportCSV;
    }

    public void setExportCSV(boolean exportCSV) {
        this.exportCSV = exportCSV;
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
    public static final String EXPORT_CSV_FLAG = "-exportCSV";


    public void setFlag(String flag, String rawValue) {
        double parsed = Double.parseDouble(rawValue);
        switch(flag) {
            case NUM_ACTIONS_FLAG:   setNumActions((int) parsed);             break;
            case REPLICAS_FLAG:      setMinNumReplicas((int) parsed);         break;
            case GAMMA_FLAG:         setGamma((float) parsed);                break;
            case ALPHA_FLAG:         setAlpha((float) parsed);                break;
            case INITIAL_T_FLAG:     setInitialT((float) parsed);             break;
            case DELTA_T_FLAG:       setDeltaT((float) parsed);               break;
            case NEIGHBORHOOD_FLAG:  setJaK((int) parsed);                    break;
            case MAX_MOVES_FLAG:     setHermesK((int) parsed);                break;
            case NUM_RESTARTS_FLAG:  setNumRestarts((int) parsed);            break;
            case ASSORTIVITY_FLAG:   setAssortivityCheckProbability(parsed);  break;
            case LATENCY_FLAG:       setLatencyCheckProbability(parsed);      break;
            case VALIDITY_FLAG:      setValidityCheckProbability(parsed);     break;
            case LOGICAL_FLAG:       setLogicalMigrationRatio(parsed);        break;
            case ITERATIONS_FLAG:    setMaxIterations((int) parsed);          break;
            case EXPORT_CSV_FLAG:    setExportCSV(parsed != 0);               break;
            default: throw new RuntimeException(flag + " is not a valid flag");
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        //General args
        builder.append(inputFile).append(' ').append(type)
                .append(" -assortivity ").append(assortivityCheckProbability)
                .append(" -delay ").append(latencyCheckProbability)
                .append(" -validity ").append(validityCheckProbability);

        if(numActions != null) {
            builder.append(" -n ").append(numActions);
        }

        //type-specific args
        if(   JABEJA_TYPE.equals(type) || JABAR_TYPE.equals(type) || J2_TYPE.equals(type) || SPAJA_TYPE.equals(type)
                || HERMES_TYPE.equals(type) || HERMAR_TYPE.equals(type) || SPARMES_TYPE.equals(type)) {
            builder.append(" -logMig ").append(logicalMigrationRatio);
        }

        if(   SPAR_TYPE.equals(type)   || SPARMES_TYPE.equals(type) || SPAJA_TYPE.equals(type)
                || RDUMMY_TYPE.equals(type) || RMETIS_TYPE.equals(type)) {
            builder.append(" -minReps ").append(minNumReplicas);
        }
        if(   JABEJA_TYPE.equals(type) || JABAR_TYPE.equals(type) ||  J2_TYPE.equals(type) || SPAJA_TYPE.equals(type)) {
            builder.append(" -alpha ").append(alpha).append(" -initT ").append(initialT)
                    .append(" -deltaT ").append(deltaT).append(" -nbhd ").append(jaK);
        }
        if(   HERMES_TYPE.equals(type) || HERMAR_TYPE.equals(type)  || SPARMES_TYPE.equals(type)) {
            builder.append(" -gamma ").append(gamma).append(" -maxIter ").append(maxIterations)
                    .append(" -maxMove ").append(hermesK);
        }
        if(   JABEJA_TYPE.equals(type)) {
            builder.append(" -restarts ").append(numRestarts);
        }

        return builder.toString();
    }

    static TraceArgs parseArgs(String[] args, Properties props) {

        String inputFolder = props.getProperty("input.folder");
        String outputFolder = props.getProperty("output.folder");

        if(args.length < 2) {
            throw new IllegalArgumentException("Must have at least 2 arguments!");
        }

        if(args.length % 2 == 1) {
            throw new IllegalArgumentException("Must have an even number of arguments!");
        }

        TraceArgs traceArgs = new TraceArgs(args[1]);
        if(args[0].contains(File.separator)) {
            traceArgs.setInputFile(args[0]);
        } else {
            traceArgs.setInputFile(inputFolder + File.separator + args[0]);
        }

        String filenameStub = generateFilename(args);
        traceArgs.setOutputFile(outputFolder + File.separator + filenameStub + ".txt");
        traceArgs.setCsvFile(outputFolder + File.separator + filenameStub + ".csv");

        if(!allowedTypes.contains(traceArgs.getType())) {
            throw new IllegalArgumentException("arg[1] must be one of " + allowedTypes);
        }

        for(int i=2; i<args.length; i += 2) {
            traceArgs.setFlag(args[i], args[i+1]);
        }

        return traceArgs;
    }

}
