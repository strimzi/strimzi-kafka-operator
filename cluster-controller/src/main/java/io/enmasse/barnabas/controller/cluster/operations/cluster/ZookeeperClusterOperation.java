package io.enmasse.barnabas.controller.cluster.operations.cluster;

public abstract class ZookeeperClusterOperation extends ClusterOperation {
    protected ZookeeperClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    protected String getLockName() {
        return "lock::zookeeper::" + namespace + "::" + name;
    }
}
