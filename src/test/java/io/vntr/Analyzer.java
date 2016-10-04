package io.vntr;

import io.vntr.hermes.HermesManager;
import io.vntr.hermes.HermesTestUtils;
import io.vntr.jabeja.JabejaManager;
import io.vntr.jabeja.JabejaTestUtils;
import io.vntr.spaja.SpajaManager;
import io.vntr.spaja.SpajaTestUtils;
import io.vntr.spar.SparManager;
import io.vntr.spar.SparTestUtils;
import io.vntr.sparmes.SparmesManager;
import io.vntr.sparmes.SparmesTestUtils;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static io.vntr.TestUtils.initSet;

/**
 * Created by robertlindquist on 10/4/16.
 */
public class Analyzer {
    private static final String MISLOVE_FACEBOOK = "/Users/robertlindquist/Documents/thesis/data/mislove_wson_2009_facebook/mislove-facebook.txt";
    private static final String LESKOVEC_FACEBOOK = "/Users/robertlindquist/Documents/thesis/data/leskovec_facebook/leskovec-facebook.txt";
    private static final String ASU_FRIENDSTER = "/Users/robertlindquist/Documents/thesis/data/asu_friendster/asu-friendster.txt";

    private static final Set<String> filenames = initSet(MISLOVE_FACEBOOK, LESKOVEC_FACEBOOK, ASU_FRIENDSTER);

    private static final int USERS_PER_PARTITION = 100;

    //TODO: commit this reenabled once it all works
//    @Test
    public void testParsing() throws Exception {
        for(String filename : filenames) {
            Map<Long, Set<Long>> friendships = TestUtils.extractFriendshipsFromFile(filename);
            JabejaManager jabejaManager = initJabejaManager(friendships);
            HermesManager hermesManager = initHermesManager(friendships);
            SparManager sparManager = initSparManager(friendships);
            SpajaManager spajaManager = initSpajaManager(friendships);
            SparmesManager sparmesManager = initSparmesManager(friendships);
        }
    }

    private SparManager initSparManager(Map<Long, Set<Long>> friendships) {
        return SparTestUtils.initGraph(2, friendships.size() / USERS_PER_PARTITION, friendships);
    }

    private HermesManager initHermesManager(Map<Long, Set<Long>> friendships) throws Exception {
        return HermesTestUtils.initGraph(1.2, friendships.size() / USERS_PER_PARTITION, friendships);
    }

    private JabejaManager initJabejaManager(Map<Long, Set<Long>> friendships) throws Exception {
        return JabejaTestUtils.initGraph(1.5, 2D, 0.2D, 9, friendships.size() / USERS_PER_PARTITION, friendships);
    }

    private SpajaManager initSpajaManager(Map<Long, Set<Long>> friendships) {
        return SpajaTestUtils.initGraph(2, 1.5, 2D, 0.2D, 9, friendships.size() / USERS_PER_PARTITION, friendships);
    }

    private SparmesManager initSparmesManager(Map<Long, Set<Long>> friendships) {
        return SparmesTestUtils.initGraph(2, 1.2, friendships.size() / USERS_PER_PARTITION, friendships);
    }

}
