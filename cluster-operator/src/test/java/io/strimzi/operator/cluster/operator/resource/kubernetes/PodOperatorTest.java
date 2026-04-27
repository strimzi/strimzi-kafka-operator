/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PodOperatorTest extends
        AbstractReadyResourceOperatorTest<KubernetesClient, Pod, PodList, PodResource> {
    @Override
    protected Class clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Pod resource(String name) {
        return new PodBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withHostname("foo")
                .endSpec()
                .build();
    }

    @Override
    protected Pod modifiedResource(String name) {
        return new PodBuilder(resource(name))
                .editSpec()
                    .withHostname("bar")
                .endSpec()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient client, MixedOperation op) {
        when(client.pods()).thenReturn(op);
    }

    @Override
    protected PodOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PodOperator(vertx, mockClient);
    }

    @Test
    void testDeletionPropagationWithoutBackgroundDeletion() {
        Vertx vertx = mock(Vertx.class);
        KubernetesClient client = mock(KubernetesClient.class);
        PodOperator podOperator = new PodOperator(vertx, client);
        assertThat(podOperator.determineDeletionPropagation(true), is(DeletionPropagation.FOREGROUND));
        assertThat(podOperator.determineDeletionPropagation(false), is(DeletionPropagation.ORPHAN));
    }

    @Test
    void testDeletionPropagationWithBackgroundDeletion() {
        Vertx vertx = mock(Vertx.class);
        KubernetesClient client = mock(KubernetesClient.class);
        PodOperator podOperator = new PodOperator(vertx, client, DeletionPropagation.BACKGROUND);
        assertThat(podOperator.determineDeletionPropagation(true), is(DeletionPropagation.BACKGROUND));
        assertThat(podOperator.determineDeletionPropagation(false), is(DeletionPropagation.ORPHAN));
    }
}
