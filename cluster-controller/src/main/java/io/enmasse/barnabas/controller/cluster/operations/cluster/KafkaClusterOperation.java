package io.enmasse.barnabas.controller.cluster.operations.cluster;

public abstract class KafkaClusterOperation extends ClusterOperation {
    protected KafkaClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    protected String getLockName() {
        return "lock::kafka::" + namespace + "::" + name;
    }
}
