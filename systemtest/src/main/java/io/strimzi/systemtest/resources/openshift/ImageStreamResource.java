/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.openshift;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceType;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class ImageStreamResource implements ResourceType<ImageStream> {

    @Override
    public String getKind() {
        return TestConstants.IMAGE_STREAM;
    }

    @Override
    public ImageStream get(String namespace, String name) {
        return imageStreamsClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(ImageStream resource) {
        imageStreamsClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(ImageStream resource) {
        imageStreamsClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(ImageStream resource) {
        imageStreamsClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(ImageStream resource) {
        return resource != null;
    }

    public static MixedOperation<ImageStream, ImageStreamList, Resource<ImageStream>> imageStreamsClient() {
        return kubeClient().getClient().adapt(OpenShiftClient.class).imageStreams();
    }
}
