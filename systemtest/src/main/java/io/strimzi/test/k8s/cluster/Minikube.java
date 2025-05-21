/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cluster;

import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * A {@link KubeCluster} implementation for {@code minikube}.
 */
public class Minikube implements KubeCluster {

    public static final String CMD = "minikube";
    private static final Logger LOGGER = LogManager.getLogger(Minikube.class);

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    @Override
    public boolean isClusterUp() {
        List<String> cmd = Arrays.asList("kubectl", "get", "nodes", "-o", "jsonpath='{.items[*].metadata.labels}'");
        try {
            return Exec.exec(cmd).out().contains("minikube.k8s.io");
        } catch (KubeClusterException e) {
            LOGGER.debug("'" + String.join(" ", cmd) + "' failed. Please double check connectivity to your cluster!");
            LOGGER.debug(e);
            return false;
        }
    }

    public String toString() {
        return CMD;
    }
}
