package io.vntr.middleware;

import io.vntr.manager.NoRepManager;

/**
 * Created by robertlindquist on 11/23/16.
 */
public class DummyMiddleware extends AbstractNoRepMiddleware {

    public DummyMiddleware(NoRepManager manager) {
        super(manager);
    }

    @Override
    public Long getMigrationTally() {
        return 0L; //No migration in dummy
    }

}
