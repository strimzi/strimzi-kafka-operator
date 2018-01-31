/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import io.fabric8.openshift.api.model.ImageStreamBuilder;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.when;

public class ImageStreamOperationsMockTest extends ResourceOperationsMockTest<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> {

    @Override
    protected Class<OpenShiftClient> clientType() {
        return OpenShiftClient.class;
    }

    @Override
    protected Class<Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected ImageStream resource() {
        return new ImageStreamBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(RESOURCE_NAME).endMetadata().build();
    }

    @Override
    protected void mocker(OpenShiftClient mockClient, MixedOperation op) {
        when(mockClient.imageStreams()).thenReturn(op);
    }

    @Override
    protected AbstractOperations<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> createResourceOperations(Vertx vertx, OpenShiftClient mockClient) {
        return new ImageStreamOperations(vertx, mockClient);
    }

}
