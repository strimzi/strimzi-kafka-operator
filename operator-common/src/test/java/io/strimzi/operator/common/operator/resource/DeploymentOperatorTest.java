/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.operator.common.Annotations;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentOperatorTest extends
        ScalableResourceOperatorTest<KubernetesClient, Deployment, DeploymentList, RollableScalableResource<Deployment>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<RollableScalableResource> resourceType() {
        return RollableScalableResource.class;
    }

    @Override
    protected Deployment resource(String name) {
        return new DeploymentBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                    .addToAnnotations(Annotations.ANNO_DEP_KUBE_IO_REVISION, "test")
                .endMetadata()
                .withNewSpec()
                    .withNewStrategy()
                        .withType("RollingUpdate")
                    .endStrategy()
                .endSpec()
                .build();
    }

    @Override
    protected Deployment modifiedResource(String name) {
        return new DeploymentBuilder(resource(name))
                .editSpec()
                    .editStrategy()
                        .withType("Recreate")
                    .endStrategy()
                .endSpec()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        AppsAPIGroupDSL mockExt = mock(AppsAPIGroupDSL.class);
        when(mockExt.deployments()).thenReturn(op);
        when(mockClient.apps()).thenReturn(mockExt);

    }

    @Override
    protected DeploymentOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new DeploymentOperator(vertx, mockClient);
    }
}
