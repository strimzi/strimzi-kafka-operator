/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.strimzi.test.Environment;
import io.strimzi.test.client.Kubernetes;
import io.strimzi.test.executor.Exec;

public class Minishift implements KubeCluster {

    private static final String CMD = "minishift";
    private static final Environment ENVIRONMENT = new Environment();

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
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
    public boolean isClusterUp() {
        try {
            String output = Exec.exec(CMD, "status").out();
            return output.contains("Minishift:  Running")
                    && output.contains("OpenShift:  Running");
        } catch (KubeClusterException e) {
            return false;
        }
    }

    @Override
    public KubeClient defaultCmdClient() {
        return new Oc();
    }

    public Kubernetes defaultClient() {
        return new Kubernetes(new DefaultOpenShiftClient(new ConfigBuilder().withMasterUrl(ENVIRONMENT.getApiUrl())
                .withOauthToken(ENVIRONMENT.getApiToken())
                .build()), "myproject");
    }

    public String toString() {
        return CMD;
    }
}
