/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.extensions.DoneableIngress;
import io.fabric8.kubernetes.api.model.extensions.Ingress;
import io.fabric8.kubernetes.api.model.extensions.IngressList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

/**
 * Operations for {@code Ingress}es.
 */
public class IngressOperator extends AbstractResourceOperator<KubernetesClient, Ingress, IngressList, DoneableIngress, Resource<Ingress, DoneableIngress>> {

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public IngressOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "Ingress");
    }

    @Override
    protected MixedOperation<Ingress, IngressList, DoneableIngress, Resource<Ingress, DoneableIngress>> operation() {
        return client.extensions().ingresses();
    }
}
