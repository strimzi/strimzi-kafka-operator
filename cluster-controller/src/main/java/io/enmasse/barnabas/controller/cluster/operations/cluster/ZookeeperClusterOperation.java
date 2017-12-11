package io.enmasse.barnabas.controller.cluster.operations.cluster;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.vertx.core.Vertx;

public abstract class ZookeeperClusterOperation extends ClusterOperation {
    protected ZookeeperClusterOperation(String namespace, String name) {
        super(namespace, name);
    }

    protected String getLockName() {
        return "lock::zookeeper::" + namespace + "::" + name;
    }
}
