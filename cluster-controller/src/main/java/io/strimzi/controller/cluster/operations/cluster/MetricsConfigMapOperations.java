/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.cluster;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.controller.cluster.operations.resource.ConfigMapOperations;
import io.vertx.core.Vertx;

public class MetricsConfigMapOperations extends ConfigMapOperations {
    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     */
    public MetricsConfigMapOperations(Vertx vertx, KubernetesClient client) {
        super(vertx, client);
    }
}
