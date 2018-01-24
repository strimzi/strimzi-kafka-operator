package io.strimzi.controller.cluster.operations;

public abstract class KafkaClusterOperation extends ClusterOperation {
    protected KafkaClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    protected String getLockName() {
        return "lock::kafka::" + namespace + "::" + name;
    }
}
