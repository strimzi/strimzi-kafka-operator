/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import static io.strimzi.test.Exec.isExecutableOnPath;
import static io.strimzi.test.Exec.exec;

public class OpenShift implements KubeCluster {

    private static final String OC = "oc";

    @Override
    public boolean isAvailable() {
        return isExecutableOnPath(OC);
    }

    @Override
    public boolean isClusterUp() {
        try {
            exec(OC, "cluster", "status");
            return true;
        } catch (KubeClusterException e) {
            if (e.statusCode == 1) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public void clusterUp() {
        exec(OC, "cluster", "up");
    }

    @Override
    public void clusterDown() {
        exec(OC, "cluster", "down");
    }

    @Override
    public KubeClient defaultClient() {
        return new Oc();
    }
}
