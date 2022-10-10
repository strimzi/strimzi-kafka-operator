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
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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

    protected static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
    }

    @Test
    public void testCreateReadUpdate(VertxTestContext context) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool", 10);
        PodOperator pr = new PodOperator(vertx, client);

        pr.list(NAMESPACE, Labels.EMPTY);
        context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY), is(emptyList())));

        Checkpoint async = context.checkpoint(1);
        pr.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, POD).onComplete(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));
            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                        .map(p -> p.getMetadata().getName())
                        .collect(Collectors.toList()), is(singletonList(RESOURCE_NAME))));

            pr.reconcile(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, RESOURCE_NAME, null).onComplete(deleteResult -> {
                context.verify(() -> assertThat(deleteResult.succeeded(), is(true)));
                async.flag();
            });
        });
    }
}
