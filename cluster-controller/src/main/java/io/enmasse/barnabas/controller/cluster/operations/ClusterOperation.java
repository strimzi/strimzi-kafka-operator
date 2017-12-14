package io.enmasse.barnabas.controller.cluster.operations;

public abstract class ClusterOperation implements Operation {
    protected final String namespace;
    protected final String name;

    protected final int LOCK_TIMEOUT = 60000;

    protected ClusterOperation(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    protected abstract String getLockName();
}
