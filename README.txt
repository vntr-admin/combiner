Welcome to combiner, a simulation of a graph database partition manager.

To build, ensure that you have maven installed, as well as Java 7 or later.
Open src/main/resources/log4j.properties and edit log4j.appender.file.File to have an acceptable logging location on your computer
Type "mvn clean package".
Edit the config.properties file to contain the input and output directories.  Optionally, if you wish to run METIS for comparison, edit the file to indicate where gpmetis can be found, and a directory it can use to write temporary files.

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

To run, use the following command:
    java -jar target/vntr.jar [filename in inputDirectory] [Type] [arguments]

    For example, running Hermes with gamma=1.15, iterationCutoffRatio=0.0025 and k=3:
        java -jar target/vntr.jar facebook_0_R.txt HERMES 1.15 0.0025 3

