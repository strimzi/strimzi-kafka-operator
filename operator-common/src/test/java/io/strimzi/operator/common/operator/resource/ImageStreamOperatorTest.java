/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.when;

public class ImageStreamOperatorTest extends AbstractNamespacedResourceOperatorTest<OpenShiftClient, ImageStream, ImageStreamList, Resource<ImageStream>> {

    @Override
    protected Class<OpenShiftClient> clientType() {
        return OpenShiftClient.class;
    }

    @Override
    protected Class<Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected ImageStream resource(String name) {
        return new ImageStreamBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                .endMetadata()
                .build();
    }

    @Override
    protected ImageStream modifiedResource(String name) {
        return new ImageStreamBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                    .addToLabels("foo", "bar")
                .endMetadata()
                .build();
    }

    @Override
    protected void mocker(OpenShiftClient mockClient, MixedOperation op) {
        when(mockClient.imageStreams()).thenReturn(op);
    }

    @Override
    protected AbstractNamespacedResourceOperator<OpenShiftClient, ImageStream, ImageStreamList, Resource<ImageStream>> createResourceOperations(Vertx vertx, OpenShiftClient mockClient) {
        return new ImageStreamOperator(vertx, mockClient);
    }

}
