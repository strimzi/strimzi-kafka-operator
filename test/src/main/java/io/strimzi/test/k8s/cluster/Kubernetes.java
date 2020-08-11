/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cluster;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import io.strimzi.test.k8s.cmdClient.Kubectl;
import io.strimzi.test.k8s.exceptions.KubeClusterException;

/**
 * A {@link KubeCluster} implementation for any {@code Kubernetes} cluster.
 */
public class Kubernetes implements KubeCluster {

    public static final String CMD = "kubectl";
    private static final String OLM_NAMESPACE = "operators";

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    @Override
    public boolean isClusterUp() {
        try {
            return Exec.exec(CMD, "cluster-info").exitStatus() && !Exec.exec(CMD, "api-resources").out().contains("openshift.io");
        } catch (KubeClusterException e) {
            LOGGER.debug("Error:", e);
            return false;
        }
    }

    @Override
    public KubeCmdClient defaultCmdClient() {
        return new Kubectl();
    }

    @Override
    public KubeClient defaultClient() {
        return new KubeClient(new DefaultKubernetesClient(CONFIG), "default");
    }

    public String toString() {
        return CMD;
    }

    @Override
    public String defaultOlmNamespace() {
        return OLM_NAMESPACE;
    }
}
