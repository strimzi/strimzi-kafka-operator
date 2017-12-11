package io.enmasse.barnabas.controller.cluster.operations;

import io.enmasse.barnabas.controller.cluster.K8SUtils;
import io.vertx.core.Vertx;

public abstract class ZookeeperClusterOperation extends ClusterOperation {
    protected ZookeeperClusterOperation(Vertx vertx, K8SUtils k8s, String namespace, String name) {
        super(vertx, k8s, namespace, name);
    }

    protected String getLockName() {
        return "lock::zookeeper::" + name;
    }
}
