/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cluster;

import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import io.strimzi.test.k8s.cmdClient.KubeCmdClient;
import io.strimzi.test.k8s.cmdClient.Oc;

public class OpenShift implements KubeCluster {

    private static final String OC = "oc";
    private static final String OLM_NAMESPACE = "openshift-operators";

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(OC);
    }

    @Override
    public boolean isClusterUp() {
        try {
            return Exec.exec(OC, "status").exitStatus();
        } catch (KubeClusterException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public KubeCmdClient defaultCmdClient() {
        return new Oc();
    }

    public String toString() {
        return OC;
    }

    @Override
    public String defaultOlmNamespace() {
        return OLM_NAMESPACE;
    }
}
