/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigBuilder;
import io.fabric8.openshift.api.model.DeploymentConfigList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.DeployableScalableResource;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.when;

public class DeploymentConfigOperatorTest extends ScalableResourceOperatorTest<OpenShiftClient, DeploymentConfig,
        DeploymentConfigList, DeployableScalableResource<DeploymentConfig>> {

    @Override
    protected Class<OpenShiftClient> clientType() {
        return OpenShiftClient.class;
    }

    @Override
    protected Class<DeployableScalableResource> resourceType() {
        return DeployableScalableResource.class;
    }

    @Override
    protected DeploymentConfig resource(String name) {
        return new DeploymentConfigBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
            .endMetadata()
            .withNewSpec()
                .withNewTemplate()
                    .withNewSpec()
                        .addToContainers(new ContainerBuilder().withImage("img").build())
                    .endSpec()
                .endTemplate()
            .endSpec().build();
    }

    @Override
    protected DeploymentConfig modifiedResource(String name) {
        return new DeploymentConfigBuilder(resource(name))
                .editSpec()
                    .editTemplate()
                        .editSpec()
                            .addToContainers(new ContainerBuilder().withImage("img2").build())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    @Override
    protected void mocker(OpenShiftClient mockClient, MixedOperation op) {
        when(mockClient.deploymentConfigs()).thenReturn(op);
    }

    @Override
    protected DeploymentConfigOperator createResourceOperations(Vertx vertx, OpenShiftClient mockClient) {
        return new DeploymentConfigOperator(vertx, mockClient);
    }
}
