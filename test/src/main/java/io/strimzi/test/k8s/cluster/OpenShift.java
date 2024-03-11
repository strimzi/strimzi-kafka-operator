/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cluster;

import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import io.strimzi.test.k8s.cmdClient.Oc;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class OpenShift implements KubeCluster {

    private static final String CMD = "oc";
    public static final String OLM_SOURCE_NAMESPACE = "openshift-marketplace";
    public static final String DEFAULT_NAMESPACE = "default";
    private static final Logger LOGGER = LogManager.getLogger(OpenShift.class);

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    @Override
    public boolean isClusterUp() {
        List<String> cmd = Arrays.asList(CMD, "status", "-n", DEFAULT_NAMESPACE);
        try {
            return Exec.exec(cmd).exitStatus() && Exec.exec(CMD, "api-versions").out().contains("openshift.io");
        } catch (KubeClusterException e) {
            LOGGER.debug("'" + String.join(" ", cmd) + "' failed. Please double check connectivity to your cluster!");
            LOGGER.debug(e);
            return false;
        }
    }

    @Override
    public KubeCmdClient defaultCmdClient() {
        return new Oc();
    }

    public String toString() {
        return CMD;
    }
}
