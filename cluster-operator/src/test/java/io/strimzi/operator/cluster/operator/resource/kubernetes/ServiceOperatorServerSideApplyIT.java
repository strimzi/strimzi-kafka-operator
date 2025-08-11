/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class ServiceOperatorServerSideApplyIT extends AbstractNamespacedResourceOperatorServerSideApplyIT<KubernetesClient, Service, ServiceList, ServiceResource<Service>> {

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, Service, ServiceList,
                ServiceResource<Service>> operator() {
        return new ServiceOperator(vertx, client, true);
    }

    @Override
    protected Service getOriginal()  {
        ServicePort servicePort = new ServicePortBuilder()
            .withName("http")
            .withProtocol("TCP")
            .withPort(80)
            .withNewTargetPort(80)
            .build();

        return new ServiceBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withLabels(singletonMap("state", "new"))
            .endMetadata()
            .withNewSpec()
                .withType("ClusterIP")
                .withSelector(singletonMap("app", "kafka"))
                .withPorts(servicePort)
            .endSpec()
            .build();
    }

    @Override
    protected Service getModified()  {
        ServicePort servicePort = new ServicePortBuilder()
            .withName("http")
            .withProtocol("TCP")
            .withPort(80)
            .withNewTargetPort(80)
            .build();

        ServicePort servicePortConflicting = new ServicePortBuilder()
            .withName("https")
            .withProtocol("TCP")
            .withPort(443)
            .withNewTargetPort(443)
            .build();

        return new ServiceBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withLabels(singletonMap("state", "modified"))
            .endMetadata()
            .withNewSpec()
                .withType("ClusterIP")
                .withSelector(singletonMap("app", "kafka"))
                .withPorts(servicePort, servicePortConflicting)
            .endSpec()
            .build();
    }

    @Override
    Service getNonConflicting() {
        ServicePort servicePort = new ServicePortBuilder()
            .withName("http")
            .withProtocol("TCP")
            .withPort(80)
            .withNewTargetPort(80)
            .build();

        return new ServiceBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withAnnotations(singletonMap("my-annotation2", "my-value2"))
            .endMetadata()
            .withNewSpec()
                .withType("ClusterIP")
                .withSelector(singletonMap("app", "kafka"))
                .withPorts(servicePort)
            .endSpec()
            .build();
    }

    @Override
    protected Service getConflicting() {
        ServicePort servicePort = new ServicePortBuilder()
            .withName("http")
            .withProtocol("TCP")
            .withPort(80)
            .withNewTargetPort(80)
            .build();

        ServicePort servicePortConflicting = new ServicePortBuilder()
            .withName("https")
            .withProtocol("TCP")
            .withPort(443)
            .withNewTargetPort(444)
            .build();

        return new ServiceBuilder()
            .withNewMetadata()
                .withName(resourceName)
                .withNamespace(namespace)
                .withLabels(singletonMap("state", "new"))
            .endMetadata()
            .withNewSpec()
                .withType("ClusterIP")
                .withSelector(singletonMap("app", "kafka"))
                .withPorts(servicePort, servicePortConflicting)
            .endSpec()
            .build();
    }

    @Override
    protected void assertResources(VertxTestContext context, Service expected, Service actual)   {
        context.verify(() -> assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName())));
        context.verify(() -> assertThat(actual.getMetadata().getNamespace(), is(expected.getMetadata().getNamespace())));
        context.verify(() -> assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels())));
        context.verify(() -> assertThat(actual.getSpec().getPorts().size(), is(expected.getSpec().getPorts().size())));
        actual.getSpec().getPorts().forEach(actualPort -> {
                context.verify(() -> assertThat(expected.getSpec().getPorts().stream().anyMatch(servicePort -> servicePort.getPort().equals(actualPort.getPort())), is(true)));
                context.verify(() -> assertThat(expected.getSpec().getPorts().stream().anyMatch(servicePort -> servicePort.getTargetPort().equals(actualPort.getTargetPort())), is(true)));
            }
        );
        context.verify(() -> assertThat(actual.getSpec().getType(), is(expected.getSpec().getType())));
    }
}
