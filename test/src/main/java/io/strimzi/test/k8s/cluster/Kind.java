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
 * Represents a Kubernetes cluster implemented using the "kind" tool.
 * This class provides methods to check the availability of the "kind" command,
 * the status of the cluster, and to retrieve default clients for interaction.
 *
 * @see KubeCluster
 */
public class Kind implements KubeCluster {

    public static final String CMD = "kind";
    private static final Logger LOGGER = LogManager.getLogger(Kind.class);

    /**
     * Determines if the "kind" command is available in the system's path.
     *
     * @return true if the "kind" command is available, false otherwise.
     */
    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    /**
     * Checks if the Kubernetes cluster managed by "kind" is up and running.
     *
     * @return true if the cluster is up, false otherwise.
     */
    @Override
    public boolean isClusterUp() {
        List<String> cmd = Arrays.asList("kubectl", "get", "nodes", "-o", "jsonpath='{.items[*].spec.providerID}'");
        try {
            return Exec.exec(cmd).out().contains("kind://");
        } catch (KubeClusterException e) {
            LOGGER.debug("'" + String.join(" ", cmd) + "' failed. Please double check connectivity to your cluster!");
            LOGGER.debug(e);
            return false;
        }
    }

    /**
     * Retrieves the default command line client for interaction with the cluster.
     *
     * @return a {@link KubeCmdClient} instance.
     */
    @Override
    public KubeCmdClient defaultCmdClient() {
        return new Kubectl();
    }

    /**
     * Retrieves the default client for programmatically interacting with the cluster.
     *
     * @return a {@link KubeClient} instance.
     */
    @Override
    public KubeClient defaultClient() {
        return new KubeClient(new KubernetesClientBuilder().withConfig(CONFIG).build(), "default");
    }

    /**
     * Returns the string representation of this class, which is the "kind" command.
     *
     * @return the "kind" command string.
     */
    public String toString() {
        return CMD;
    }
}
