/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.when;

public class ServiceOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<Resource> resourceType() {
        return Resource.class;
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
}
