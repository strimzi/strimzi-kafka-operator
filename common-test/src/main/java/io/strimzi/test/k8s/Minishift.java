/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

public class Minishift implements KubeCluster {
    public static final String CMD = "minishift";

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    @Override
    public void clusterUp() {
        Exec.exec(CMD, "start");
    }

    @Override
    public void clusterDown() {
        Exec.exec(CMD, "stop");
    }

    @Override
    public boolean isClusterUp() {
        try {
            String output = Exec.exec(CMD, "status").out();
            return output.contains("Minishift:  Running")
                    && output.contains("OpenShift:  Running");
        } catch (KubeClusterException e) {
            return false;
        }
    }

    @Override
    public KubeClient defaultClient() {
        return new Oc();
    }

    public String toString() {
        return CMD;
    }
}
