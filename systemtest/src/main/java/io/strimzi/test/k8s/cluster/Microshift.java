/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cluster;

import io.skodjob.testframe.clients.KubeClusterException;
import io.skodjob.testframe.executor.Exec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * A {@link KubeCluster} implementation for any {@code Kubernetes} cluster.
 */
public class Microshift implements KubeCluster {

    public static final String CMD = "oc";
    private static final Logger LOGGER = LogManager.getLogger(Microshift.class);

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    @Override
    public boolean isClusterUp() {
        List<String> cmd = Arrays.asList(CMD, "cluster-info");
        try {
            return Exec.exec(cmd).exitStatus() && Exec.exec(CMD, "api-versions").out().contains("openshift.io");
        } catch (KubeClusterException e) {
            LOGGER.debug("'{}' failed. Please double check connectivity to your cluster!", String.join(" ", cmd));
            LOGGER.debug(e);
            return false;
        }
    }

    public String toString() {
        return "microshift";
    }
}
