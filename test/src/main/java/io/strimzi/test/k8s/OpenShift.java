/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OpenShift implements KubeCluster {

    private static final Logger LOGGER = LogManager.getLogger(OpenShift.class);

    private static final String OC = "oc";

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(OC);
    }

    @Override
    public boolean isClusterUp() {
        try {
            Exec.exec(OC, "status");
            return true;
        } catch (KubeClusterException e) {
            if (e.result.exitStatus() == 1) {
                if (e.result.out().contains("not yet ready")) {
                    LOGGER.debug("Waiting for oc cluster to finish coming up");
                    // In this case it is still coming up, so wait for rather than saying it's not up
                    TestUtils.waitFor("oc cluster up", 1_000, 60_000, () -> {
                        try {
                            Exec.exec(OC, "cluster", "status");
                            LOGGER.trace("oc cluster is up");
                            return true;
                        } catch (KubeClusterException e2) {
                            LOGGER.trace("oc cluster still not up");
                            return false;
                        }
                    });
                    return true;
                }
                return false;
            }
            throw e;
        }
    }

    @Override
    public void clusterUp() {
        Exec.exec(OC, "cluster", "up");
    }

    @Override
    public void clusterDown() {
        Exec.exec(OC, "cluster", "down");
    }

    @Override
    public KubeClient defaultClient() {
        return new Oc();
    }

    public String toString() {
        return OC;
    }
}
