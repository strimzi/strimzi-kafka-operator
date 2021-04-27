/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.storage.StorageClass;
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder;
import io.fabric8.kubernetes.api.model.storage.StorageClassList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.StorageAPIGroupDSL;
import io.vertx.core.Vertx;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageClassOperatorTest extends AbstractNonNamespacedResourceOperatorTest<KubernetesClient,
        StorageClass, StorageClassList, Resource<StorageClass>> {

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        StorageAPIGroupDSL mockStorage = mock(StorageAPIGroupDSL.class);
        when(mockClient.storage()).thenReturn(mockStorage);
        when(mockStorage.storageClasses()).thenReturn(op);
    }

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient, StorageClass, StorageClassList,
            Resource<StorageClass>> createResourceOperations(
                    Vertx vertx, KubernetesClient mockClient) {
        return new StorageClassOperator(vertx, mockClient) {
            @Override
            protected long deleteTimeoutMs() {
                return 100;
            }
        };
    }

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected StorageClass resource() {
        return new StorageClassBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("foo", "bar"))
                .endMetadata()
                .withAllowVolumeExpansion(true)
                .withReclaimPolicy("Delete")
                .withProvisioner("kubernetes.io/aws-ebs")
                .withParameters(singletonMap("type", "gp2"))
            .build();
    }

    @Override
    protected StorageClass modifiedResource() {
        return new StorageClassBuilder(resource())
                .withParameters(singletonMap("type", "st1"))
            .build();
    }
}
