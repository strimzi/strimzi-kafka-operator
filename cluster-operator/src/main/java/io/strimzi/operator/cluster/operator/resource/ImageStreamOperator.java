/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.DoneableImageStream;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.vertx.core.Vertx;

/**
 * Operations for {@code ImageStream}s.
 */
public class ImageStreamOperator extends AbstractResourceOperator<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The OpenShift client
     */
    public ImageStreamOperator(Vertx vertx, OpenShiftClient client) {
        super(vertx, client, "ImageStream");
    }

    @Override
    protected MixedOperation<ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> operation() {
        return client.imageStreams();
    }
}
