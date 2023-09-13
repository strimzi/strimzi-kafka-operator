/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cluster;

import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import io.strimzi.test.k8s.cmdClient.Kubectl;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * The `Kind` class provides functionality for interacting with a Kubernetes cluster created using the Kind tool.
 * Kind is a tool for running local Kubernetes clusters using Docker container nodes.
 * This class implements the `KubeCluster` interface and provides methods for checking the availability of the Kind tool,
 * verifying if a Kind cluster is up and running, and creating default Kubernetes clients and command clients for the cluster.
 *
 * Usage:
 * - Check if Kind is available on the system using {@link #isAvailable()}.
 * - Check if a Kind cluster is up and running using {@link #isClusterUp()}.
 * - Obtain a default Kubernetes command client using {@link #defaultCmdClient()}.
 * - Obtain a default Kubernetes client using {@link #defaultClient()}.
 *
 * @see KubeCluster
 * @see KubeCmdClient
 * @see KubeClient
 */
public class Kind implements KubeCluster {

    public static final String CMD = "kind";
    private static final Logger LOGGER = LogManager.getLogger(Kind.class);

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    @Override
    public boolean isClusterUp() {
        List<String> cmd = Arrays.asList(CMD, "status");
        try {
            return Exec.exec(cmd).exitStatus();
        } catch (KubeClusterException e) {
            LOGGER.debug("'" + String.join(" ", cmd) + "' failed. Please double check connectivity to your cluster!");
            LOGGER.debug(e);
            return false;
        }
    }

    @Override
    public KubeCmdClient defaultCmdClient() {
        return new Kubectl();
    }

    @Override
    public KubeClient defaultClient() {
        return new KubeClient(new KubernetesClientBuilder().withConfig(CONFIG).build(), "default");
    }

    public String toString() {
        return CMD;
    }
}
