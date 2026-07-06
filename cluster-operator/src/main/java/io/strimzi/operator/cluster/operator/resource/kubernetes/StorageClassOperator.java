/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNonNamespacedResourceOperator;

import java.util.concurrent.Executor;

/**
 * Operator for managing storage classes
 */
public class StorageClassOperator extends AbstractNonNamespacedResourceOperator<KubernetesClient,
        StorageClass, StorageClassList, Resource<StorageClass>> {
    /**
     * Constructor.
     *
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client The Kubernetes client.
     */

    public StorageClassOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "StorageClass");
    }

    @Override
    protected NonNamespaceOperation<StorageClass, StorageClassList,
                Resource<StorageClass>> operation() {
        return client.storage().v1().storageClasses();
    }
}
