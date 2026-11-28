/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ServiceAccountResource;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;

import java.util.concurrent.Executor;

/**
 * Operator for managing Service Accounts
 */
public class ServiceAccountOperator extends AbstractNamespacedResourceOperator<KubernetesClient, ServiceAccount, ServiceAccountList, ServiceAccountResource> {
    /**
     * Constructor
     * @param asyncExecutor         Executor to use for asynchronous subroutines
     * @param client                The Kubernetes client
     */
    public ServiceAccountOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "ServiceAccount", true);
    }

    @Override
    protected MixedOperation<ServiceAccount, ServiceAccountList, ServiceAccountResource> operation() {
        return client.serviceAccounts();
    }
}
