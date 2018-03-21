/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigBuilder;
import io.fabric8.openshift.api.model.DeploymentConfigList;
import io.fabric8.openshift.api.model.DoneableDeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.DeployableScalableResource;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.when;

public class DeploymentConfigOperationsMockTest extends ScalableResourceOperationsMockTest<OpenShiftClient, DeploymentConfig,
        DeploymentConfigList, DoneableDeploymentConfig,
        DeployableScalableResource<DeploymentConfig, DoneableDeploymentConfig>> {

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
    protected DeploymentConfigOperations createResourceOperations(Vertx vertx, OpenShiftClient mockClient) {
        return new DeploymentConfigOperations(vertx, mockClient);
    }
}
