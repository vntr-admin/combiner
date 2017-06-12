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
            Note: we include one set of datasets by default, in the
            [checkout_location]/data directory.  It is a derivative of the
            friendship graph from J. McAuley and J. Leskovec. Learning to
            Discover Social Circles in Ego Networks. NIPS, 2012.
        output.folder: set it to where you want the output stored


Building:
    From [checkout_location], run the following:
        mvn clean package


METIS-specific Configuration (Optional):

Downloading and building METIS:
    http://glaros.dtc.umn.edu/gkhome/metis/metis/download
    Note: You'll need a C99-compatible compiler and cmake.
    Note: We do not provide any support for this process.
    Note: if you are using a Debian-based GNU Linux distribution, then the
    command "apt-get install metis" should install it to /usr/bin/gpmetis .

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
                 DUMMY, RDUMMY, RMETIS

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
            In log lines where no check occurs, it is reported as -99.
            0 <= p <= 1.  Default is 1 (always).
            Note that assortivity is not a function of the type, and should
            be the same for all runs at the same point.

        -exportCSV x
            whether to export the run in csv in addition to tabular format.
            default value is 1 (true), allowed values are 0 (false) and 1.

    type-specific args: specific to your choice of type (see next section).


    Examples:
        Running Spar with default values:
            java -jar target/vntr.jar facebook_0_R.txt SPAR

        Running METIS on the first 999 actions with an absolute path:
            java -jar target/vntr.jar /home/root/karate_1_S.txt METIS -n 999


****************************************************************************
************************* TYPE-SPECIFIC PARAMETERS *************************
****************************************************************************


There are ten types of partition managers you can simulate:

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
            Default is 3.  Sane values are 1-10.
        -logMig l
            weighting to give logical migrations in oscillation calculation.
            E.g., with l=0.2, 100 logical moves and 30 physical ones, the
            reported oscillation would be (100 * 0.2) + 30 = 50.
            0.0 <= l <= 1.0.  Default is 0.

Hermes:
    Description: Migrates vertices independently and allows imbalanced
        partitions (within constraints).  No replication.
    Type name: HERMES
    Arguments:
        -gamma g
            the allowed imbalance ratio.  If average partition size is 100
            users, and gamma=1.25, partitions will contain 75-125 users.
            1 < g < 2.  Default value is 1.1.  Sane values are 1.01-1.25.
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
        -logMig l
            weighting to give logical migrations in oscillation calculation.
            E.g., with l=0.2, 100 logical moves and 30 physical ones, the
            reported oscillation would be (100 * 0.2) + 30 = 50.  
            0.0 <= l <= 1.0.  Default is 0.

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
            t > 1.0.  Default is 2.  Sane values are 1.5-2.5.
        -deltaT dT
            How much the temperature decreases each iteration.
            dT > 0.0.  Default is 0.5.  Sane values are (initialT-1)/x,
            where 2 <= x <= 40.
            Note: smaller values improve quality but take longer.
            E.g. 0.1 takes ~2x as long as 0.2.
        -nbhd k
            size of neighborhood to search for a better partitioning.
            (Integer) k >= 1.  Default is 15.  Sane values are 3-21.
        -logMig l
            weighting to give logical migrations in oscillation calculation.
            E.g., with l=0.2, 100 logical moves and 30 physical ones, the
            reported oscillation would be (100 * 0.2) + 30 = 50.  
            0.0 <= l <= 1.0.  Default is 0.

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
            1 < g < 2.  Default value is 1.05.  Sane values are 1.01-1.25.
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
        -logMig l
            weighting to give logical migrations in oscillation calculation.
            E.g., with l=0.2, 100 logical moves and 30 physical ones, the
            reported oscillation would be (100 * 0.2) + 30 = 50.  
            0.0 <= l <= 1.0.  Default is 0.


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
        -logMig l
            weighting to give logical migrations in oscillation calculation.
            E.g., with l=0.2, 100 logical moves and 30 physical ones, the
            reported oscillation would be (100 * 0.2) + 30 = 50.  
            0.0 <= l <= 1.0.  Default is 0.

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
        -logMig l
            weighting to give logical migrations in oscillation calculation.
            E.g., with l=0.2, 100 logical moves and 30 physical ones, the
            reported oscillation would be (100 * 0.2) + 30 = 50.  
            0.0 <= l <= 1.0.  Default is 0.

Dummy Repartitioner:
    Description: Kind of like METIS or Ja-be-ja but without repartitioning,
    or any tactical moves of users to reduce edge cut.  Provides a baseline
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

Replica METIS Repartitioner
    Description: Kind of like SPAR, but repartitions with METIS after downtime
    and some befriend operations (at random), and then generates partitions
    that meet the k-replication and replica-colocation constraints.
    Type Name: RMETIS
    Arguments:
        -minReps n
            the minimum number of replicas to maintain of each user.
            (Integer) n >= 0.  Default is 0.  Sane values are 0-3.
            Note: you need to use a trace that has at least as many replicas
            as you specify or it could crash.

****************************************************************************
**************************** INPUT FILE FORMAT *****************************
****************************************************************************

Input File Format:

A typical input trace looks like this (perhaps named facebook_0_hard.txt):

F: {0=[1, 2, 3], 1=[2, 4, 6], ..., 4038=[]}
PIDS: [0, 1, ..., 32]
P: {0=[2561, 2306, ...], 1=[2560, 2563, ...], ..., 32=[1, 8, ...]}
R: {0=[1, 8, ...], 1=[2561, 2306, ...], ..., 32=[2560, 2563, ...]}
A0    : UNFRIEND 3302 3321
A1    : BEFRIEND 467 2951
A2    : ADD_USER 4040 -1
A3    : REMOVE_USER 2782 -1
A4    : DOWNTIME -1 -1
A5    : REMOVE_PARTITION 21 -1
A6    : ADD_PARTITION 34 -1
...
A1000 : BEFRIEND 226 2419


We typically name the file as follows:
    [dataset]_[maximum -minRep supported]_[any_suffix_that_makes_sense].txt

The content of the file is broken into two sections:
    (1) The starting conditions, which includes:
        (a) The initial friendship graph (F)
        (b) The partition IDs (PIDS)
        (c) The initial partitioning (P)
        (d) The initial replicas (R), which are ignored by all replica-free
            systems (those that don't support the -minReps argument)

    (2) A sequence of actions to be performed on the graph.

Starting Conditions:

F, P and R are all maps from a key to a set of values.
The format of these maps is Java 5+'s default Map::toString() method.
PIDS is a set, and similarly uses Java 5+'s default Set::toString() method. 

These maps reference two types of IDs: partitions IDs (called pids in the
code), and user IDs (called uids).  The system expects the set of uids to go
from 0 to (number of users)-1, inclusive, and the set of pids to go from 0 
to (number of partitions)-1, inclusive.

Within each map, the keys and values can be in any order, though it has not
been tested with keys that are not sorted in increasing order.

F, which stands for Friendships, should contain each uid as a key, and the
value for that key should be a set containing all of the IDs of that user's
friends who have a higher uid than that user.  E.g., as above, user 0 has
user 1's ID in its set, but not vice-versa.  Make sure that all users have a
key-value pair in F, even if they have no friends, or at least no friends
with larger IDs.

PIDS is a set of the values of the partition IDs.  It shouldn't require the
pids to be sorted, but it hasn't been tested with them in unsorted order.

P, which stands for partition (masters), should contain each pid as a key,
and the value for that key should be the set of all users initially on that
partition.  Ensure that all pids appear as a key in this map, even if they
have no users.  Also note that, as a partitioning, each uid should be in
precisely one partition.

R, which stands for replicas, should contain each pid as a key and the value
for that key should be the set of all users who have a replica on that
partition.  Ensure that all pids appear as a key in this map, even if they
have no replicas.  Also note that, if you intend to use this dataset with
minReps > 0, the initial replica partitions should meet that criterion, or
the behavior is undefined.

Actions:

After these initial lines are a set of operations to be performed on the
dataset.  These are all of the following format:

A[number] : [ACTION] [ID_1] [ID_2]

The action number should start at zero and continue in contiguous, sorted
order to (number of Actions) - 1.
The ACTION should be one of: {ADD_USER, REMOVE_USER, BEFRIEND, UNFRIEND,
ADD_PARTITION, REMOVE_PARTITION, DOWNTIME}.
The interpretation of ID_1 and ID_2 depends on the action.  In general,
actions that use both IDs don't care about the order, actions using one ID
look at ID_1, and actions that don't use IDs ignore both.  It is customary
to set unnecessary ID values to -1.

The IDs are interpreted as follows:
ADD_USER [uid] [N/A]
REMOVE_USER [uid] [N/A]
BEFRIEND [uid_1] [uid_2]
UNFRIEND [uid_1] [uid_2]
ADD_PARTITION [pid] [N/A]
REMOVE_PARTITION [pid] [N/A]
DOWNTIME [N/A] [N/A]

When generating actions, ensure that you don't refer to uids or pids that do
not exist or no longer exist.  Also ensure that you don't remove all pids.
On a side note, the system can behave strangely with a very small number of
partitions (e.g. fewer than five).  For this reason we suggest having at
least ten to start and not going below seven or so.


****************************************************************************
**************************** OUTPUT FILE FORMAT ****************************
****************************************************************************

The system outputs in two formats: TXT and CSV, the latter of which can be
disabled with the argument -exportCSV 0
The TXT is intended to be human-readable, while the CSV is meant to be
imported, whether into Excel, Matlab, Mathematica or other programs.

TXT:
The first line contains all of the arguments used to run the trace,
including the default values if appropriate.
The bulk of the file consists of a fixed-width 120 character table with a
header row, one row per action, printed BEFORE performing the action, and a
final row that shows the state of the system after the last action (which
contains the special END action).

Column   Meaning                       Width  When Could it Overflow?
No       Action number                 8      > 99,999,999 Actions
Type     Which algorithm (e.g. METIS)  8      Never
Date     Actual Time on PC             20     100,000 AD
Action   Action to be performed*       15     e.g. len(ID_1)+len(ID_2) > 10
Ps       Number of Partitions          4      > 9,999 Partitions
Nodes    Number of Users               6      > 999,999 Users
Edges    Number of Friendships         8      > 99,999,999 Friendships
Assort.  Assortivity                   8      Never
EdgeCut  Edge Cut                      8      > 99,999,999 Edges Cut
Replicas Number of Replicas            9      > 999,999,999 Replicas
Moves    Cumulative Migration Count**  8      > 99,999,999 Migrations
Delay    Expected Query Delay          6      Never

* This is in an abbreviated format, which can be interpreted as follows:
** Note that the migration count does not include migrations that occur as
part of a partition removal, as those cannot be avoided.

Val  Action
+U   ADD_USER
-U   REMOVE_USER
+F   BEFRIEND
-F   UNFRIEND
+P   ADD_PARTITION
-P   REMOVE_PARTITION
DT   DOWNTIME
 
The final section of the TXT file is a table that shows the cumulative
impact of each action type on the resulting edge cut and number of replicas.


CSV:
A typical CSV file with a header row and (number of actions)+1 content rows.
The column meanings are as follows:

Column  Meaning                     Equivalent Column in TXT
index   Action number               No
numP    Number of Partitions        Ps
numU    Number of Users             Nodes
numF    Number of Friendships       Edges
asrt    Assortivity                 Assort.
cut     Edge Cut                    EdgeCut
reps    Number of Replicas          Replicas
moves   Cumulative Migration Count  Moves
delay   Expected Query Delay        Delay

Note that Type, Date, and Action are omitted from the CSV.
