/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.strimzi.test.client.Kubernetes;
import io.strimzi.test.executor.Exec;

import static io.strimzi.test.k8s.Minishift.CONFIG;

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
            return Exec.exec(CMD, "status").exitStatus() == 0;
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
    public KubeClient defaultCmdClient() {
        return new Kubectl();
    }

    public Kubernetes defaultClient() {
        return new Kubernetes(new DefaultKubernetesClient(CONFIG), "default");
    }

    public String toString() {
        return CMD;
    }

}
