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

import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigBuilder;
import io.fabric8.openshift.api.model.DeploymentConfigList;
import io.fabric8.openshift.api.model.DoneableDeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.DeployableScalableResource;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentConfigOperationsMockTest extends ResourceOperationsMockTest<OpenShiftClient,DeploymentConfig,DeploymentConfigList,DoneableDeploymentConfig,DeployableScalableResource<DeploymentConfig,DoneableDeploymentConfig>> {

    @Override
    protected Class<OpenShiftClient> clientType() {
        return OpenShiftClient.class;
    }

    @Override
    protected Class<ScalableResource> resourceType() {
        return ScalableResource.class;
    }

    @Override
    protected DeploymentConfig resource() {
        return new DeploymentConfigBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(RESOURCE_NAME).endMetadata().build();
    }

    @Override
    protected void mocker(OpenShiftClient mockClient, MixedOperation op) {
        /*ExtensionsAPIGroupDSL mockExt = mock(ExtensionsAPIGroupDSL.class);
        when(mockExt.deployments()).thenReturn(op);
        when(mockClient.extensions()).thenReturn(mockExt);*/
        when(mockClient.deploymentConfigs()).thenReturn(op);
    }

    @Override
    protected AbstractOperations<OpenShiftClient, DeploymentConfig, DeploymentConfigList, DoneableDeploymentConfig, DeployableScalableResource<DeploymentConfig, DoneableDeploymentConfig>> createResourceOperations(Vertx vertx, OpenShiftClient mockClient) {
        return new DeploymentConfigOperations(vertx, mockClient);
    }
}
