/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Operator for managing Service Accounts
 */
public class ServiceAccountOperator extends AbstractNamespacedResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, Resource<ServiceAccount>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public ServiceAccountOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, "ServiceAccount");
    }

    @Override
    protected MixedOperation<ServiceAccount, ServiceAccountList, Resource<ServiceAccount>> operation() {
        return client.serviceAccounts();
    }

    @Override
    protected Future<ReconcileResult<ServiceAccount>> internalPatch(Reconciliation reconciliation, String namespace, String name, ServiceAccount current, ServiceAccount desired) {
        if (desired.getSecrets() == null || desired.getSecrets().isEmpty())    {
            desired.setSecrets(current.getSecrets());
        }

        if (desired.getImagePullSecrets() == null || desired.getImagePullSecrets().isEmpty())    {
            desired.setImagePullSecrets(current.getImagePullSecrets());
        }

        return super.internalPatch(reconciliation, namespace, name, current, desired);
    }
}
