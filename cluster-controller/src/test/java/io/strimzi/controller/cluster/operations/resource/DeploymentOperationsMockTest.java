/*
 * Copyright 2018, Strimzi Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentOperationsMockTest extends ResourceOperationsMockTest<KubernetesClient, Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<ScalableResource> resourceType() {
        return ScalableResource.class;
    }

    @Override
    protected Deployment resource() {
        return new DeploymentBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(RESOURCE_NAME).endMetadata().build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        ExtensionsAPIGroupDSL mockExt = mock(ExtensionsAPIGroupDSL.class);
        when(mockExt.deployments()).thenReturn(op);
        when(mockClient.extensions()).thenReturn(mockExt);

    }

    @Override
    protected AbstractOperations<KubernetesClient, Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new DeploymentOperations(vertx, mockClient);
    }
}
