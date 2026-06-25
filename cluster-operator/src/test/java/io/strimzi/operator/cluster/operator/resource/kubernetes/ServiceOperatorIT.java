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
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.concurrent.AbstractNamespacedResourceOperatorIT;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ServiceOperatorIT extends AbstractNamespacedResourceOperatorIT<KubernetesClient, Service, ServiceList, ServiceResource<Service>> {

    @Override
    public AbstractNamespacedResourceOperator<KubernetesClient, Service, ServiceList,
                ServiceResource<Service>> operator() {
        return new ServiceOperator(asyncExecutor, client, false);
    }

    @Override
    public Service getOriginal()  {
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
    public Service getModified()  {
        ServicePort servicePort = new ServicePortBuilder()
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
                    .withPorts(servicePort)
                .endSpec()
                .build();
    }

    @Override
    public void assertResources(Service expected, Service actual)   {
        assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName()));
        assertThat(actual.getMetadata().getNamespace(), is(expected.getMetadata().getNamespace()));
        assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels()));
        assertThat(actual.getSpec().getPorts().get(0).getPort(), is(expected.getSpec().getPorts().get(0).getPort()));
    }
}
