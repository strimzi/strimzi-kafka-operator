/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.controller.cluster.operations.resource;

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
public class ImageStreamOperations extends AbstractOperations<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> {
    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The OpenShift client
     */
    public ImageStreamOperations(Vertx vertx, OpenShiftClient client) {
        super(vertx, client, "ImageStream");
    }

    @Override
    protected MixedOperation<ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> operation() {
        return client.imageStreams();
    }
}
