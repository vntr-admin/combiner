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
    From [checkout_location], run the following:
        mvn clean package


METIS-specific Configuration (Optional):

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
    java -jar target/vntr.jar file type [common args] [type-specific args]

Arguments
    file: the name of the input file
        If not an absolute path, it will in the input.folder directory as
        specified in config.properties.  Otherwise, loads the absolute path.

    type: one of JABEJA, JABAR, HERMES, HERMAR, SPAR, SPAJA, SPARMES, METIS,
                 DUMMY, RDUMMY

    common args:
        -n numActions
            limits the run to the first numActions actions (so you needn't
            run the whole trace).

        -validity p
            checks the validity of the partition states with probability p.
            Throws a RuntimeException if any invalid state is found.
            0 <= p <= 1.  Default is 0 (never check).

        -delay p
            calculates the expected average query delay of the system with
            probability p.
            In log lines where no check occurs, delay is reported as -99.
            0 <= p <= 1.  Default is 1 (always).

        -assortivity p
            calculates the assortivity of the friendship graph with
            probability p.
            In log lines where no check occurrs, it is reported as -99.
            0 <= p <= 1.  Default is 1 (always).
            Note that assortivity is not a function of the type, and should
            be the same for all runs at the same point.

    type-specific args: specific to your choice of type (see next section).


    Examples:
        Running Spar with default values:
            java -jar target/vntr.jar facebook_0_R.txt SPAR

        Running METIS on the first 999 actions with an absolute path:
            java -jar target/vntr.jar /home/root/karate_1_S.txt METIS -n 999


****************************************************************************
************************* TYPE-SPECIFIC PARAMETERS *************************
****************************************************************************


There are six types of partition managers you can simulate:

Ja-be-Ja:
    Description: Swaps vertices, uses simulated annealing.  No replication.
    Type name: JABEJA
    Arguments:
        -alpha a
            raises the comparison function to a.
            (E.g. 1 is like Manhattan distance, 2 like Euclidean, etc.)
            a >= 1.0.  Default is 3.  Sane values are 1-4.
        -initT t
            The initial Temperature.  The distance function is multiplied by
            t (which decreases over time) to allow for temporary deviations
            from hill-climbing.
            t > 0.0.  Default is 2.  Sane values are 1.5-2.5.
        -deltaT dT
            How much the temperature decreases each iteration.
            dT > 0.0.  Default is 0.025.  Sane values are (initialT-1)/x,
            where 2 <= x <= 40.
            Note: smaller values improve quality but take longer.
            E.g. 0.1 takes ~2x as long as 0.2.
        -nbhd k
            size of neighborhood to search for a better partitioning.
            (Integer) k >= 1.  Default is 15.  Sane values are 3-21.
        -restarts
            number of restarts allowed for the repartitioning algorithm.
            Default is 10.  Sane values are 1-15.

Hermes:
    Description: Migrates vertices independently and allows imbalanced
        partitions (within constraints).  No replication.
    Type name: HERMES
    Arguments:
        -gamma g
            the allowed imbalance ratio.  If average partition size is 100
            users, and gamma=1.25, partitions will contain 75-125 users.
            1 < g < 2.  Default value is 1.15.  Sane values are 1.01-1.25.
        -maxIter r
            Maximum number of iterations allowed in the repartitioning
            algorithm.  (integer) r > 0.  Default is 100.  Sane values are
            1-250.
            Smaller values speed up process but might negatively affect
            partition quality.
        -maxMove m
            How many users can move off one partition in one iteration.
            (Integer) m >= 1.  Default is 3.  Sane values are 1-3.
            Smaller values take longer but might avoid oscillation.

Spar:
    Description: Ensures full replication, and minimizes replicas upon
        befriending by comparing results of migrating user A to user B's
        partition, vice versa, or no migration.
    Type name: SPAR
    Arguments:
        -minReps n
            the minimum number of replicas to maintain of each user.
            (Integer) n >= 0.  Default is 0.  Sane values are 0-3.
            Note: you need to use a trace that has at least as many replicas
            as you specify or it could crash.

Spaja:
    Description: Spar normally + Ja-be-Ja upon downtime.
    Differs from Ja-be-Ja in that instead of maximizing number of users on
    resultant partition, it minimizes replicas.
    Type name: SPAJA
    Arguments:
        -minReps n
            the minimum number of replicas to maintain of each user.
            (Integer) n >= 0.  Default is 0.  Sane values are 0-3.
            Note: you need to use a trace that has at least as many replicas
            as you specify or it could crash.
        -alpha a
            raises the comparison function to a.
            (E.g. 1 is like Manhattan distance, 2 like Euclidean, etc.)
            a >= 1.0.  Default is 1.  Sane values are 1-4.
        -initT t
            The initial temperature.  The distance function is multiplied by
            t (which decreases over time) to allow for temporary deviations
            from hill-climbing.
            t > 0.0.  Default is 2.  Sane values are 1.5-2.5.
        -deltaT dT
            How much the temperature decreases each iteration.
            dT > 0.0.  Default is 0.5.  Sane values are (initialT-1)/x,
            where 2 <= x <= 40.
            Note: smaller values improve quality but take longer.
            E.g. 0.1 takes ~2x as long as 0.2.
        -nbhd k
            size of neighborhood to search for a better partitioning.
            (Integer) k >= 1.  Default is 15.  Sane values are 3-21.

Sparmes:
    Description: Spar normally + Hermes upon downtime.
    Differs from Hermes in that instead of setting gain to how many more
    friends a user has on the new partition, it sets it to the reduction in
    replicas due to the migration.  Limits iteration count to 100.
    Type name: SPARMES
    Arguments:
        -minReps n
            the minimum number of replicas to maintain of each user.
            (Integer) n >= 0.  Default is 0.  Sane values are 0-3.
            Note: you need to use a trace that has at least as many replicas
            as you specify or it could crash.
        -gamma g
            the allowed imbalance ratio.  If average partition size is 100
            users, and gamma=1.25, partitions will contain 75-125 users.
            1 < g < 2.  Default value is 1.15.  Sane values are 1.01-1.25.
        -maxMove m
            How many users can move off one partition in one iteration.
            (Integer) m >= 1.  Default is 3.  Sane values are 1-3.
            Smaller values take longer but might avoid oscillation.
        -maxIter r
            Maximum number of iterations allowed in the repartitioning
            algorithm.  (integer) r > 0.  Default is 100.  Sane values are
            1-250.
            Smaller values speed up process but might negatively affect
            partition quality.


Metis:
    Description: Uses Karypis et alia's METIS algorithm
        Provided for comparison with a high-quality, performant,
        off-the-shelf partitioner.
        Requires installation of metis (tested with version 5.1.0) and
        subsequent setup in config.properties.  (See "METIS-specific
        Configuration" under CONFIGURATION AND BUILDING.)
    Type name: METIS
    Arguments:
        [none]

Jabar:
    Description: Ja-be-Ja normally + SPAR upon befriending, but only one
    repartitioning restart, and no randomization of initial partitions
    (allowing it to repartition incrementally).  No replication.
    Type name: JABAR
    Arguments:
        -alpha a
            raises the comparison function to a.
            (E.g. 1 is like Manhattan distance, 2 like Euclidean, etc.)
            a >= 1.0.  Default is 3.  Sane values are 1-4.
        -initT t
            The initial Temperature.  The distance function is multiplied by
            t (which decreases over time) to allow for temporary deviations
            from hill-climbing.
            t > 0.0.  Default is 2.  Sane values are 1.5-2.5.
        -deltaT dT
            How much the temperature decreases each iteration.
            dT > 0.0.  Default is 0.025.  Sane values are (initialT-1)/x,
            where 2 <= x <= 40.
            Note: smaller values improve quality but take longer.
            E.g. 0.1 takes ~2x as long as 0.2.
        -nbhd k
            size of neighborhood to search for a better partitioning.
            (Integer) k >= 1.  Default is 15.  Sane values are 3-21.

Hermar:
    Description: Hermes normally + SPAR upon befriending, but only
    repartitions upon downtime, not user addition.  No replication.
    Type Name: HERMAR
    Arguments:
        -gamma g
            the allowed imbalance ratio.  If average partition size is 100
            users, and gamma=1.25, partitions will contain 75-125 users.
            1 < g < 2.  Default value is 1.15.  Sane values are 1.01-1.25.
        -maxIter r
            Maximum number of iterations allowed in the repartitioning
            algorithm.  (integer) r > 0.  Default is 100.  Sane values are
            1-250.
            Smaller values speed up process but might negatively affect
            partition quality.
        -maxMove m
            How many users can move off one partition in one iteration.
            (Integer) m >= 1.  Default is 3.  Sane values are 1-3.
            Smaller values take longer but might avoid oscillation.

Dummy Repartitioner:
    Description: Kind of like METIS or Jabeja but without repartitioning, or
    any tactical moves of users to reduce edge cut.  Provides a baseline
    against which to compare partitioning quality.  No replication.
    Type Name: DUMMY
    Arguments:
        [none]

Replica Dummy Repartitioner:
    Description: Kind of like SPAR, but without moving users upon
    befriending, or in fact at any time other than partition removal.
    Type Name: RDUMMY
    Arguments:
        -minReps n
            the minimum number of replicas to maintain of each user.
            (Integer) n >= 0.  Default is 0.  Sane values are 0-3.
            Note: you need to use a trace that has at least as many replicas
            as you specify or it could crash.
