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
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

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
    public void testNodePortPatching()  {
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

        assertThat(current.getSpec().getPorts().get(0).getNodePort(), is(desired.getSpec().getPorts().get(1).getNodePort()));
        assertThat(current.getSpec().getPorts().get(1).getNodePort(), is(desired.getSpec().getPorts().get(0).getNodePort()));
    }

    @Test
    void testCattleAnnotationPatching() {
        KubernetesClient client = mock(KubernetesClient.class);

        Map<String, String> currentAnnotations = Map.of(
                "field.cattle.io~1publicEndpoints", "foo",
                "cattle.io/test", "bar",
                "some-other", "baz"
        );

        Service current = new ServiceBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                    .withAnnotations(currentAnnotations)
                .endMetadata()
                .withNewSpec()
                    .withType("LoadBalancer")
                .endSpec()
                .build();

        Service desired = new ServiceBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withType("LoadBalancer")
                .endSpec()
                .build();

        ServiceOperator op = new ServiceOperator(vertx, client);
        op.internalPatch(NAMESPACE, RESOURCE_NAME, current, desired);

        assertThat(desired.getMetadata().getAnnotations().get("field.cattle.io~1publicEndpoints"), equalTo("foo"));
        assertThat(desired.getMetadata().getAnnotations().get("cattle.io/test"), equalTo("bar"));
        assertThat(desired.getMetadata().getAnnotations().containsKey("some-other"), is(false));
    }

    @Test
    public void testHealthCheckPortPatching()  {
        KubernetesClient client = mock(KubernetesClient.class);

        Service current = new ServiceBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withType("LoadBalancer")
                    .withHealthCheckNodePort(34321)
                .endSpec()
                .build();

        Service desired = new ServiceBuilder()
                .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                .withType("LoadBalancer")
                .endSpec()
                .build();

        ServiceOperator op = new ServiceOperator(vertx, client);
        op.patchHealthCheckPorts(current, desired);

        assertThat(current.getSpec().getHealthCheckNodePort(), is(desired.getSpec().getHealthCheckNodePort()));
    }

    @Test
    public void testIpFamilyPatching()  {
        KubernetesClient client = mock(KubernetesClient.class);

        Service current = new ServiceBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIp")
                    .withPorts(
                            new ServicePortBuilder()
                                    .withName("port1")
                                    .withPort(1234)
                                    .withTargetPort(new IntOrString(1234))
                                    .build(),
                            new ServicePortBuilder()
                                    .withName("port2")
                                    .withPort(5678)
                                    .withTargetPort(new IntOrString(5678))
                                    .build()
                    )
                    .withNewIpFamily("IPv6")
                .endSpec()
                .build();

        Service current2 = new ServiceBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIp")
                    .withPorts(
                            new ServicePortBuilder()
                                    .withName("port1")
                                    .withPort(1234)
                                    .withTargetPort(new IntOrString(1234))
                                    .build(),
                            new ServicePortBuilder()
                                    .withName("port2")
                                    .withPort(5678)
                                    .withTargetPort(new IntOrString(5678))
                                    .build()
                    )
                    .withNewIpFamily("IPv6")
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
        op.patchIpFamily(current, desired);

        assertThat(current.getSpec().getIpFamily(), is(desired.getSpec().getIpFamily()));
        assertThat(current2.getSpec().getIpFamily(), is(desired.getSpec().getIpFamily()));
    }
}
