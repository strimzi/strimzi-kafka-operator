/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import static org.mockito.Mockito.when;

public class PvcOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected PersistentVolumeClaim resource() {
        return new PersistentVolumeClaimBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(RESOURCE_NAME).endMetadata().build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        when(mockClient.persistentVolumeClaims()).thenReturn(op);
    }

    @Override
    protected PvcOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PvcOperator(vertx, mockClient);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void createWhenExistsIsAPatch(TestContext context) {
        super.createWhenExistsIsAPatch(context);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void successfulCreation(TestContext context) {
        super.successfulCreation(context);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void creationThrows(TestContext context) {
        super.creationThrows(context);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void existenceCheckThrows(TestContext context) {
        super.existenceCheckThrows(context);
    }
}
