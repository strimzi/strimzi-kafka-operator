/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class PodOperatorMockTest {
    public static final String RESOURCE_NAME = "my-resource";
    public static final String NAMESPACE = "test";
    private final static Pod POD = new PodBuilder()
                    .withNewMetadata()
                        .withName(RESOURCE_NAME)
                        .withNamespace(NAMESPACE)
                    .endMetadata()
                    .withNewSpec()
                        .withContainers(new ContainerBuilder()
                                .withName("busybox")
                                .withImage("my-image:latest")
                                .withCommand("sleep", "3600")
                                .withImagePullPolicy("IfNotPresent")
                                .build())
                        .withRestartPolicy("Never")
                        .withTerminationGracePeriodSeconds(1L)
                    .endSpec()
                    .build();

    private static KubernetesClient client;
    private static MockKube3 mockKube;

    protected static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;

    @BeforeAll
    public static void beforeAll() {
        mockKube = new MockKube3.MockKube3Builder()
                .withPodController()
                .withDeletionController()
                .withNamespaces(NAMESPACE)
                .build();
        mockKube.start();
        client = mockKube.client();

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
    }

    @AfterAll
    public static void afterAll() {
        sharedWorkerExecutor.close();
        vertx.close();

        mockKube.stop();
    }

    @Test
    public void testCreateReadUpdate(VertxTestContext context) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool", 10);
        PodOperator pr = new PodOperator(vertx, client);

        Checkpoint async = context.checkpoint(1);
        pr.listAsync(NAMESPACE, Labels.EMPTY);
        pr.listAsync(NAMESPACE, Labels.EMPTY)
                .onComplete(context.succeeding(podList -> context.verify(() -> assertThat(podList.stream().map(p -> p.getMetadata().getName()).collect(Collectors.toList()), is(List.of())))))
                .compose(i -> pr.createOrUpdate(new Reconciliation("test", "kind", NAMESPACE, RESOURCE_NAME), POD))
                .onComplete(context.succeeding(createResult -> context.verify(() -> { })))
                .compose(i -> pr.listAsync(NAMESPACE, Labels.EMPTY))
                .onComplete(context.succeeding(podList -> context.verify(() -> assertThat(podList.stream().map(p -> p.getMetadata().getName()).collect(Collectors.toList()), is(singletonList(RESOURCE_NAME))))))
                .compose(i -> pr.reconcile(new Reconciliation("test", "kind", NAMESPACE, RESOURCE_NAME), NAMESPACE, RESOURCE_NAME, null))
                .onComplete(context.succeeding(deleteResult -> context.verify(async::flag)));
    }
}
