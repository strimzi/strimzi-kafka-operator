/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;

import java.util.concurrent.Executor;

/**
 * Operations for {@code PersistentVolumeClaim}s.
 */
public class PvcOperator extends AbstractNamespacedResourceOperator<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> {
    /**
     * Constructor
     * @param asyncExecutor         Executor to use for asynchronous subroutines
     * @param client                The Kubernetes client
     */
    public PvcOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "PersistentVolumeClaim", true);
    }

    @Override
    protected MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> operation() {
        return client.persistentVolumeClaims();
    }
}
