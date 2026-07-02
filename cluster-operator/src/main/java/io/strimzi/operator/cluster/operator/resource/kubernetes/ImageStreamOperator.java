/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.operator.common.operator.resource.kubernetes.AbstractNamespacedResourceOperator;

import java.util.concurrent.Executor;

/**
 * Operations for {@code ImageStream}s.
 */
public class ImageStreamOperator extends AbstractNamespacedResourceOperator<OpenShiftClient, ImageStream, ImageStreamList, Resource<ImageStream>> {
    /**
     * Constructor
     * @param asyncExecutor Executor to use for asynchronous subroutines
     * @param client The OpenShift client
     */
    public ImageStreamOperator(Executor asyncExecutor, OpenShiftClient client) {
        super(asyncExecutor, client, "ImageStream");
    }

    @Override
    protected MixedOperation<ImageStream, ImageStreamList, Resource<ImageStream>> operation() {
        return client.imageStreams();
    }
}
