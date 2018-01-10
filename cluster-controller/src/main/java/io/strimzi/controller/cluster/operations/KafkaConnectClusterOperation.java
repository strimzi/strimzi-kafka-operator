package io.strimzi.controller.cluster.operations;

public abstract class KafkaConnectClusterOperation extends ClusterOperation {
    protected KafkaConnectClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    protected String getLockName() {
        return "lock::kafka-connect::" + namespace + "::" + name;
    }
}
