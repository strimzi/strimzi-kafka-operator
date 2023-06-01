/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;

/**
 * Operations for {@code ImageStream}s.
 */
public class ImageStreamOperator extends AbstractNamespacedResourceOperator<OpenShiftClient, ImageStream, ImageStreamList, Resource<ImageStream>> {
    /**
     * Constructor
     * @param client The OpenShift client
     */
    public ImageStreamOperator(OpenShiftClient client) {
        super(client, "ImageStream");
    }

    @Override
    protected MixedOperation<ImageStream, ImageStreamList, Resource<ImageStream>> operation() {
        return client.imageStreams();
    }
}
