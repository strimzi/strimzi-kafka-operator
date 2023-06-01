/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class PodOperatorMockTest {
    public static final String RESOURCE_NAME = "my-resource";
    public static final String NAMESPACE = "test";
    private final static Pod POD = new PodBuilder()
            .withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(RESOURCE_NAME)
            .endMetadata()
            .withNewSpec()
                .withHostname("foo")
            .endSpec()
            .build();

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;

    @Test
    public void testCreateReadUpdate(VertxTestContext context) {
        PodOperator pr = new PodOperator(client);

        pr.list(NAMESPACE, Labels.EMPTY);
        context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY), is(emptyList())));

        Checkpoint async = context.checkpoint(1);
        pr.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, POD)
            .whenComplete((createResult, e1) -> {
                context.verify(() -> assertThat(e1, is(nullValue())));
                context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                            .map(p -> p.getMetadata().getName())
                            .collect(Collectors.toList()), is(singletonList(RESOURCE_NAME))));

                pr.reconcile(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, RESOURCE_NAME, null)
                    .whenComplete((deleteResult, e2) -> {
                        context.verify(() -> assertThat(e2, is(nullValue())));
                        async.flag();
                    });
            });
    }
}
