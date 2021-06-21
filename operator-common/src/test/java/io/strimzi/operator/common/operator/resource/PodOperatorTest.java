/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.mockkube.MockKube;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

public class PodOperatorTest extends
        AbstractReadyResourceOperatorTest<KubernetesClient, Pod, PodList, PodResource<Pod>> {
    @Test
    public void testCreateReadUpdate(VertxTestContext context) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool", 10);
        KubernetesClient client = new MockKube().build();
        PodOperator pr = new PodOperator(vertx, client);

        pr.list(NAMESPACE, Labels.EMPTY);
        context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY), is(emptyList())));

        pr.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, resource()).onComplete(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));
            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                        .map(p -> p.getMetadata().getName())
                        .collect(Collectors.toList()), is(singletonList(RESOURCE_NAME))));

            pr.reconcile(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, RESOURCE_NAME, null)
                    .onComplete(deleteResult -> context.verify(() -> assertThat(deleteResult.succeeded(), is(true))));

            context.completeNow();
        });
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected Class clientType() {
        return KubernetesClient.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected Class<? extends Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Pod resource() {
        return new PodBuilder()
                .withNewMetadata()
                    .withNamespace(NAMESPACE)
                    .withName(RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withHostname("foo")
                .endSpec()
                .build();
    }

    @Override
    protected Pod modifiedResource() {
        return new PodBuilder(resource())
                .editSpec()
                    .withHostname("bar")
                .endSpec()
                .build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    protected void mocker(KubernetesClient client, MixedOperation op) {
        when(client.pods()).thenReturn(op);
    }

    @Override
    protected PodOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new PodOperator(vertx, mockClient);
    }
}
