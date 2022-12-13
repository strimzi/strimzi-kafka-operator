/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigBuilder;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.BuildTriggerPolicy;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.when;

public class BuildConfigOperatorTest extends AbstractNamespacedResourceOperatorTest<OpenShiftClient, BuildConfig,
        BuildConfigList, BuildConfigResource<BuildConfig, Void, Build>> {

    @Override
    protected void mocker(OpenShiftClient mockClient, MixedOperation mockCms) {
        when(mockClient.buildConfigs()).thenReturn(mockCms);
    }

    @Override
    protected BuildConfigOperator createResourceOperations(Vertx vertx, OpenShiftClient mockClient) {
        return new BuildConfigOperator(vertx, mockClient);
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
    protected BuildConfig resource(String name) {
        return new BuildConfigBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
            .endMetadata()
            .withNewSpec()
                .withTriggers(new BuildTriggerPolicy())
            .endSpec().build();
    }

    @Override
    protected BuildConfig modifiedResource(String name) {
        return new BuildConfigBuilder(resource(name))
                .editSpec()
                    .withServiceAccount("service-account")
                .endSpec()
                .build();
    }

    @Override
    @Test
    public void testCreateWhenExistsWithChangeIsAPatch(VertxTestContext context) {
        testCreateWhenExistsWithChangeIsAPatch(context, false);
    }

    @Override
    @Test
    public void testReconcileDeleteDoesNotTimeoutWhenResourceIsAlreadyDeleted(VertxTestContext context) {
        assumeTrue(false, "BuildConfigOperator does not use self-closing watch so this test should be skipped");
    }
}
