/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.vertx.core.Vertx;

public class CrdOperator<C extends KubernetesClient,
            T extends CustomResource,
            L extends CustomResourceList<T>,
            D extends Doneable<T>>
        extends AbstractWatchableResourceOperator<C, T, L, D, Resource<T, D>> {

    private final Class<T> cls;
    private final Class<L> listCls;
    private final Class<D> doneableCls;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public CrdOperator(Vertx vertx, C client, Class<T> cls, Class<L> listCls, Class<D> doneableCls) {
        super(vertx, client, Crds.kind(cls));
        this.cls = cls;
        this.listCls = listCls;
        this.doneableCls = doneableCls;
    }

    @Override
    protected MixedOperation<T, L, D, Resource<T, D>> operation() {
        return Crds.operation(client, cls, listCls, doneableCls);
    }

    public static void main(String[] args) {
        System.out.println("This is a test");
    }
}
