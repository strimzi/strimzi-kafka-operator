/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.cluster.operations.resource;

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

public class PvcOperationsMockTest extends ResourceOperationsMockTest<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> {

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
    protected PvcOperations createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PvcOperations(vertx, mockClient);
    }

    @Override
    @Test(expected = UnsupportedOperationException.class)
    public void createWhenExistsIsANop(TestContext context) {
        super.createWhenExistsIsANop(context);
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
