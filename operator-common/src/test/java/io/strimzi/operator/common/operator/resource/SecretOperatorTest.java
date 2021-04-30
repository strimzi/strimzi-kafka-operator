/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.when;

public class SecretOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, Secret, SecretList, Resource<Secret>> {


    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Secret resource() {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(NAMESPACE)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withData(singletonMap("FOO", "BAR"))
                .build();
    }

    @Override
    protected Secret modifiedResource() {
        return new SecretBuilder(resource())
                .withData(singletonMap("FOO2", "BAR2"))
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        when(mockClient.secrets()).thenReturn(op);
    }

    @Override
    protected AbstractResourceOperator<KubernetesClient, Secret, SecretList, Resource<Secret>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new SecretOperator(vertx, mockClient);
    }
}
