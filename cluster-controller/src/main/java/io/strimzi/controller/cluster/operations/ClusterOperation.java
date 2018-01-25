package io.strimzi.controller.cluster.operations;

import io.strimzi.controller.cluster.K8SUtils;

public abstract class ClusterOperation implements Operation<K8SUtils> {
    protected final String namespace;
    protected final String name;

    protected final int LOCK_TIMEOUT = 60000;

    protected ClusterOperation(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    protected abstract String getLockName();
}
