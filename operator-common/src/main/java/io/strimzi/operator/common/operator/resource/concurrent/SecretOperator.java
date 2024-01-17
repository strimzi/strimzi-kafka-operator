/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource.concurrent;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import java.util.concurrent.Executor;

/**
 * Operations for {@code Secret}s.
 */
public class SecretOperator extends AbstractNamespacedResourceOperator<KubernetesClient, Secret, SecretList, Resource<Secret>> {

    /**
     * Constructor
     *
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client        The Kubernetes client
     */
    public SecretOperator(Executor asyncExecutor, KubernetesClient client) {
        super(asyncExecutor, client, "Secret");
    }

    @Override
    protected MixedOperation<Secret, SecretList, Resource<Secret>> operation() {
        return client.secrets();
    }
}
