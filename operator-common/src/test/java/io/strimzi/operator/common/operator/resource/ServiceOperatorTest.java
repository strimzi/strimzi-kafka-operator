/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, Service, ServiceList, DoneableService, ServiceResource<Service, DoneableService>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<ServiceResource> resourceType() {
        return ServiceResource.class;
    }

    @Override
    protected Service resource() {
        return new ServiceBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(RESOURCE_NAME).endMetadata().build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        when(mockClient.services()).thenReturn(op);
    }

    @Override
    protected ServiceOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new ServiceOperator(vertx, mockClient);
    }

    @Test
    public void testNodePortPatching(TestContext context)  {
        KubernetesClient client = mock(KubernetesClient.class);

        Service current = new ServiceBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withType("NodePort")
                    .withPorts(
                            new ServicePortBuilder()
                                    .withName("port1")
                                    .withPort(1234)
                                    .withTargetPort(new IntOrString(1234))
                                    .withNodePort(31234)
                                    .build(),
                            new ServicePortBuilder()
                                    .withName("port2")
                                    .withPort(5678)
                                    .withTargetPort(new IntOrString(5678))
                                    .withNodePort(35678)
                                    .build()
                    )
                .endSpec()
                .build();

        Service desired = new ServiceBuilder()
                .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                .withType("NodePort")
                .withPorts(
                        new ServicePortBuilder()
                                .withName("port2")
                                .withPort(5678)
                                .withTargetPort(new IntOrString(5678))
                                .build(),
                        new ServicePortBuilder()
                                .withName("port1")
                                .withPort(1234)
                                .withTargetPort(new IntOrString(1234))
                                .build()
                )
                .endSpec()
                .build();

        ServiceOperator op = new ServiceOperator(vertx, client);
        op.patchNodePorts(current, desired);

        assertEquals(current.getSpec().getPorts().get(0).getNodePort(), desired.getSpec().getPorts().get(1).getNodePort());
        assertEquals(current.getSpec().getPorts().get(1).getNodePort(), desired.getSpec().getPorts().get(0).getNodePort());
    }
}
