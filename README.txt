Welcome to combiner, a simulation of a graph database partition manager.

****************************************************************************
************************ CONFIGURATION AND BUILDING ************************
****************************************************************************

First ensure that you have maven installed, as well as Java 7 or later.

Configuration:

    [checkout_location]/src/main/resources/log4j.properties
        log4j.appender.file.File: set to an acceptable logging
        location on your computer

    [checkout_location]/config.properties
        input.folder: set it to where you have your datasets
            Note: if you only use absolute paths in your run targets,
            you can skip this.
        output.folder: set it to where you want the output stored

Building:
    From the [checkout_location] directory, run the following:
        mvn clean package

METIS-specific Configuration (Optional)

Downloading and building METIS:
    http://glaros.dtc.umn.edu/gkhome/metis/metis/download
    Note: You'll need a C99-compatible compiler and cmake.
    Note: We do not provide any support for this process.

Configuration:
    [checkout_location]/config.properties
        gpmetis.location: the location of the gpmetis executable.
        gpmetis.tempdir: a directory it can use to write temporary files.


****************************************************************************
********************************* RUNNING **********************************
****************************************************************************

To run, use the following command:
    java -jar target/vntr.jar filename type [flags] [parameters]

Arguments
    filename: the name of the input file
        If not an absolute path, it will in the input.folder directory as
        specified in config.properties.  Otherwise, loads the absolute path.

    type: one of JABEJA, HERMES, SPAR, SPAJA, SPARMES or METIS

    flags: currently just -n numActions, which limits the run to the first
        numActions actions (so you needn't run the whole trace).

    parameters: type-specific parameters (see next section).

    For example, running Spar with minNumReplicas=0:
        java -jar target/vntr.jar facebook_0_R.txt SPAR 0

    Running METIS on the first 999 actions with an absolute path:
        java -jar target/vntr.jar /home/root/karate_1_S.txt METIS -n 999


****************************************************************************
************************* TYPE-SPECIFIC PARAMETERS *************************
****************************************************************************


There are six types of partition managers you can simulate, and their details are below:

Ja-be-Ja: Swaps vertices and uses simulated annealing.  Does not allow replication.
    Type name: JABEJA
    Arguments:
        alpha: the power to which the comparison function is raised.
            Sane values are 1-3.
        initialT: the initial Temperature (multiplicative).
            Sane values are 1.5-2.5.
        deltaT: the stepwise decrease in temperature.
            Sane values are (initialT-1)/x, where 2 <= x <= 40.
            Note: smaller values improve quality but take longer.  E.g. 0.1 takes ~2x as long as 0.2.
        befriendInitialT: same as initialT, but for the befriending step.
            Sane values are 1.1-2.
        befriendDeltaT: same as deltaT, but for the befriending step.
            Sane values are (befriendInitialT-1)/x, where 2 <= x <= 8.
        k: size of neighborhood to search for a better partitioning.
            Sane values are 3-21.

Hermes: Migrates vertices independently and allows imbalanced partitions (within constraints).  Does not allow replication.
    Type name: HERMES
    Arguments:
        gamma: the allowed imbalance ratio.  If average partition size is 100 users, and gamma=1.25, partitions will contain between 75 and 125 users.
            Sane values are 1.01-1.25.
        iterationCutoffRatio: will end migration process after NUM_USERS * iterationCutoffRatio iterations.
            OPTIONAL.  Sane values are 0.025-0.25.
        k: the maximum number of users to migrate from one partition in one iteration.
            OPTIONAL (default=3).  Sane values are 1-3.  Smaller values take longer but might avoid oscillation.

Spar: Ensures full replication, and minimizes replicas upon befriending by comparing results of migrating user A to user B's partition, vice versa, or no migration.
    Type name: SPAR
    Arguments:
        minNumReplicas: the minimum number of replicas to maintain of each user.
            Note: you need to use a trace that has at least minNumReplicas to start, or it could crash.
            Sane values are 0-3.

Spaja: Spar normally + Ja-be-Ja upon downtime.  Differs from Ja-be-Ja in that instead of maximizing number of users on resultant partition, it minimizes replicas.
    Type name: SPAJA
    Arguments:
        minNumReplicas: the minimum number of replicas to maintain of each user.  
            Note: you need to use a trace that has at least minNumReplicas to start, or it could crash.
            Sane values are 0-3.
        alpha: the power to which the comparison function is raised.
            Sane values are 1-3.
        initialT: the initial Temperature (divisive, instead of multiplicative, as we're minimizing, not maximizing).
            Sane values are 1.5-2.5.
        deltaT: the stepwise decrease in temperature.
            Sane values are (initialT-1)/x, where 2 <= x <= 40.
            Note: smaller values improve quality but take longer.  E.g. 0.1 takes ~2x as long as 0.2.
        k: size of neighborhood to search for a better partitioning.
            Sane values are 3-21.

Sparmes: Spar normally + Hermes upon downtime.  Differs from Hermes in that instead of setting gain to how many more friends a user has on the new partition, it sets it to the reduction in replicas due to the migration.  Limits iteration count to 100.
    Type name: SPARMES
    Arguments:
        minNumReplicas: the minimum number of replicas to maintain of each user.
            Note: you need to use a trace that has at least minNumReplicas to start, or it could crash.
            Sane values are 0-3.
        gamma: the allowed imbalance ratio.  If average partition size is 100 users, and gamma=1.25, partitions will contain between 75 and 125 users.
            Sane values are 1.01-1.25.

Metis: Uses Karypis et alia's METIS algorithm (provided for comparison with high-quality, performant, off-the-shelf partitioner).  Requires installation of metis (tested with version 5.1.0) and subsequent setup in config.properties.
    Type name: METIS
    Arguments:
        [none]

