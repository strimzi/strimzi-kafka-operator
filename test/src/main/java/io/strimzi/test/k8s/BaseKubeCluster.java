/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

public abstract class BaseKubeCluster implements KubeCluster {
    protected final String context;
    protected final String cmd;

    protected BaseKubeCluster(String cmd, String context) {
        this.cmd = cmd;
        this.context = context;
    }

    @Override
    public boolean isAvailable() {
        if (!Exec.isExecutableOnPath(cmd)) {
            return false;
        }
        return context == null
            || Exec.exec(cmd, "config", "get-contexts", context).exitStatus() == 0;
    }

    @Override
    public boolean isClusterUp() {
        try {
            return Exec.exec(cmd, "--context", context, "--namespace", "kube-system", "get", "pods").exitStatus() == 0;
        } catch (KubeClusterException e) {
            return false;
        }
    }

    public String toString() {
        return cmd + '(' + context + ')';
    }
}
