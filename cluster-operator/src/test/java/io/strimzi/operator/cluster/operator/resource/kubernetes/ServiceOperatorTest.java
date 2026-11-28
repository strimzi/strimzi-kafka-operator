/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperatorTest;

import static org.mockito.Mockito.when;

public class ServiceOperatorTest extends AbstractNamespacedResourceOperatorTest<KubernetesClient, Service, ServiceList, ServiceResource<Service>> {

    @Override
    protected boolean alwaysUsesServerSideApply() {
        return true;
    }

    @Override
    protected boolean supportsServerSideApply() {
        return alwaysUsesServerSideApply();
    }

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<ServiceResource> resourceType() {
        return ServiceResource.class;
    }

    @Override
    protected Service resource(String name) {
        return new ServiceBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withType("LoadBalancer")
                .endSpec()
                .build();
    }

    @Override
    protected Service modifiedResource(String name) {
        return new ServiceBuilder(resource(name))
                .editSpec()
                    .withType("NodePort")
                .endSpec()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation<Service, ServiceList, ServiceResource<Service>> op) {
        when(mockClient.services()).thenReturn(op);
    }

    @Override
    protected AbstractNamespacedResourceOperator<KubernetesClient, Service, ServiceList, ServiceResource<Service>> createResourceOperations(KubernetesClient mockClient) {
        return new ServiceOperator(asyncExecutor, mockClient);
    }
}
