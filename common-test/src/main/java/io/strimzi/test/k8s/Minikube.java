/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

/**
 * A {@link KubeCluster} implementation for {@code minikube} and {@code minishift}.
 */
public class Minikube implements KubeCluster {

    public static final String MINIKUBE = "minikube";
    public static final String MINISHIFT = "minishift";

    private final String cmd;

    private Minikube(String cmd) {
        this.cmd = cmd;
    }

    public static Minikube minikube() {
        return new Minikube(MINIKUBE);
    }

    public static Minikube minishift() {
        return new Minikube(MINISHIFT);
    }

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(cmd);
    }

    @Override
    public boolean isClusterUp() {
        try {
            String output = Exec.exec(cmd, "status").out();
            return output.contains("minikube: Running")
                    && output.contains("cluster: Running")
                    && output.contains("kubectl: Correctly Configured:");
        } catch (KubeClusterException e) {
            return false;
        }
    }

    @Override
    public void clusterUp() {
        Exec.exec(cmd, "start");
    }

    @Override
    public void clusterDown() {
        Exec.exec(cmd, "stop");
    }

    @Override
    public KubeClient defaultClient() {
        return MINIKUBE.equals(cmd) ? new Kubectl() : new Oc();
    }
}
