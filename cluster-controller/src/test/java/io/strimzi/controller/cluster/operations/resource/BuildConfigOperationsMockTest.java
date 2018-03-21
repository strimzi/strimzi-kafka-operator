/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.operations.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.DoneableBuildConfig;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.vertx.core.Vertx;

import static org.mockito.Mockito.when;

public class BuildConfigOperationsMockTest extends ResourceOperationsMockTest<OpenShiftClient, BuildConfig,
        BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>, Void> {

    @Override
    protected void mocker(OpenShiftClient mockClient, MixedOperation mockCms) {
        when(mockClient.buildConfigs()).thenReturn(mockCms);
    }

    @Override
    protected BuildConfigOperations createResourceOperations(Vertx vertx, OpenShiftClient mockClient) {
        return new BuildConfigOperations(vertx, mockClient);
    }

    @Override
    protected Class<OpenShiftClient> clientType() {
        return OpenShiftClient.class;
    }

    @Override
    protected Class<BuildConfigResource> resourceType() {
        return BuildConfigResource.class;
    }

    @Override
    protected BuildConfig resource() {
        return new BuildConfigBuilder().withNewMetadata().withNamespace(NAMESPACE).withName(RESOURCE_NAME).endMetadata().build();
    }
}
