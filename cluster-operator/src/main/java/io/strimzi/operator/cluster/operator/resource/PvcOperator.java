/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operations for {@code PersistentVolumeClaim}s.
 */
public class PvcOperator extends AbstractResourceOperator<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public PvcOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "PersistentVolumeClaim");
    }

    @Override
    protected MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> operation() {
        return client.persistentVolumeClaims();
    }

    @Override
    public Future<ReconcileResult<PersistentVolumeClaim>> reconcile(String namespace, String name, PersistentVolumeClaim resource) {
        if (resource != null) {
            throw new UnsupportedOperationException(); // should never happen
        } else {
            return super.reconcile(namespace, name, null);
        }
    }
}
