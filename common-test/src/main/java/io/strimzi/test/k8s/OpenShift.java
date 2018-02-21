/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

public class OpenShift implements KubeCluster {

    private static final String OC = "oc";

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(OC);
    }

    @Override
    public boolean isClusterUp() {
        try {
            Exec.exec(OC, "cluster", "status");
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
}
