/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

/**
 * A {@link KubeCluster} implementation for {@code minikube} and {@code minishift}.
 */
public class Minikube implements KubeCluster {

    public static final String CMD = "minikube";

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    @Override
    public boolean isClusterUp() {
        try {
            String output = Exec.exec(CMD, "status").out();
            return output.contains("minikube: Running")
                    && output.contains("cluster: Running")
                    && output.contains("kubectl: Correctly Configured:");
        } catch (KubeClusterException e) {
            return false;
        }
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
    public KubeClient defaultClient() {
        return new Kubectl();
    }

    public String toString() {
        return CMD;
    }

}
