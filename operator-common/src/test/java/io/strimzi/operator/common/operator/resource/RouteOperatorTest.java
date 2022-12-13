/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.fabric8.openshift.api.model.RouteList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.when;

public class RouteOperatorTest extends AbstractNamespacedResourceOperatorTest<OpenShiftClient, Route, RouteList, Resource<Route>> {
    @Override
    protected Class<OpenShiftClient> clientType() {
        return OpenShiftClient.class;
    }

    @Override
    protected Class<Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Route resource(String name) {
        return new RouteBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                .endMetadata()
                .build();
    }

    @Override
    protected Route modifiedResource(String name) {
        return new RouteBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                    .addToLabels("foo", "bar")
                .endMetadata()
                .build();
    }

    @Override
    protected void mocker(OpenShiftClient mockClient, MixedOperation op) {
        when(mockClient.routes()).thenReturn(op);
    }

    @Override
    protected AbstractNamespacedResourceOperator<OpenShiftClient, Route, RouteList, Resource<Route>> createResourceOperations(Vertx vertx, OpenShiftClient mockClient) {
        return new RouteOperator(vertx, mockClient);
    }
}
