/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperatorTest;

import static org.mockito.Mockito.when;

public class PvcOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> {

    @Override
    protected boolean alwaysUsesServerSideApply() {
        return true;
    }

    @Override
    protected boolean supportsServerSideApply() {
        return alwaysUsesServerSideApply();
    }

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected PersistentVolumeClaim resource(String name) {
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                .endMetadata()
                .build();
    }

    @Override
    protected PersistentVolumeClaim modifiedResource(String name) {
        return new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                    .addToLabels("foo", "bar")
                .endMetadata()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> op) {
        when(mockClient.persistentVolumeClaims()).thenReturn(op);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, Resource<PersistentVolumeClaim>> createResourceOperations(KubernetesClient mockClient) {
        return new PvcOperator(asyncExecutor, mockClient);
    }
}
