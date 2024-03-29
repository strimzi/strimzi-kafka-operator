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
     * @param useServerSideApply Whether to use server side apply
     */
    public SecretOperator(Executor asyncExecutor, KubernetesClient client, boolean useServerSideApply) {
        super(asyncExecutor, client, "Secret", useServerSideApply);
    }

    @Override
    protected MixedOperation<Secret, SecretList, Resource<Secret>> operation() {
        return client.secrets();
    }
}
