/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.openshift;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.BuildList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildResource;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceType;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class BuildConfigResource implements ResourceType<BuildConfig> {

    @Override
    public String getKind() {
        return TestConstants.BUILD_CONFIG;
    }

    @Override
    public BuildConfig get(String namespace, String name) {
        return buildConfigClient().inNamespace(namespace).withName(name).get();
    }

    @Override
    public void create(BuildConfig resource) {
        buildConfigClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(BuildConfig resource) {
        buildConfigClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName()).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
    }

    @Override
    public void update(BuildConfig resource) {
        buildConfigClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public boolean waitForReadiness(BuildConfig resource) {
        return resource != null;
    }

    public static MixedOperation<BuildConfig, BuildConfigList, io.fabric8.openshift.client.dsl.BuildConfigResource<BuildConfig, Void, Build>> buildConfigClient() {
        return kubeClient().getClient().adapt(OpenShiftClient.class).buildConfigs();
    }

    public static MixedOperation<Build, BuildList, BuildResource> buildsClient() {
        return kubeClient().getClient().adapt(OpenShiftClient.class).builds();
    }
}
