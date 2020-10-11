/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.DeploymentConfig;
import io.fabric8.openshift.api.model.DeploymentConfigBuilder;
import io.fabric8.openshift.api.model.DeploymentConfigList;
import io.fabric8.openshift.api.model.DoneableDeploymentConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.DeployableScalableResource;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.when;

public class DeploymentConfigOperatorTest extends ScalableResourceOperatorTest<OpenShiftClient, DeploymentConfig,
        DeploymentConfigList, DoneableDeploymentConfig,
        DeployableScalableResource<DeploymentConfig, DoneableDeploymentConfig>> {

    @Override
    protected Class<OpenShiftClient> clientType() {
        return OpenShiftClient.class;
    }

    @Override
    protected Class<DeployableScalableResource> resourceType() {
        return DeployableScalableResource.class;
    }

    @Override
    protected DeploymentConfig resource() {
        return new DeploymentConfigBuilder().withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(RESOURCE_NAME)
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
    protected void mocker(OpenShiftClient mockClient, MixedOperation op) {
        when(mockClient.deploymentConfigs()).thenReturn(op);
    }

    @Override
    protected DeploymentConfigOperator createResourceOperations(Vertx vertx, OpenShiftClient mockClient) {
        return new DeploymentConfigOperator(vertx, mockClient);
    }

    @Test
    @Override
    public void testWaitUntilReadySuccessfulImmediately(VertxTestContext context) {
        // This test has to be skipped because of https://github.com/fabric8io/kubernetes-client/issues/2537 which
        // doesn't allow easy mocking of the readiness states
        // TODO: This dummy method should be removed after https://github.com/fabric8io/kubernetes-client/issues/2537 is fixed
        context.completeNow();

    }

    @Test
    @Override
    public void testWaitUntilReadySuccessfulAfterOneCall(VertxTestContext context) {
        // This test has to be skipped because of https://github.com/fabric8io/kubernetes-client/issues/2537 which
        // doesn't allow easy mocking of the readiness states
        // TODO: This dummy method should be removed after https://github.com/fabric8io/kubernetes-client/issues/2537 is fixed
        context.completeNow();
    }

    @Test
    @Override
    public void testWaitUntilReadySuccessfulAfterTwoCalls(VertxTestContext context) {
        // This test has to be skipped because of https://github.com/fabric8io/kubernetes-client/issues/2537 which
        // doesn't allow easy mocking of the readiness states
        // TODO: This dummy method should be removed after https://github.com/fabric8io/kubernetes-client/issues/2537 is fixed
        context.completeNow();
    }
}
