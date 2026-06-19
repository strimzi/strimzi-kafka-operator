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
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube3.MockKube3;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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

    private static final Executor ASYNC_EXECUTOR = ForkJoinPool.commonPool();

    @BeforeAll
    public static void beforeAll() {
        mockKube = new MockKube3.MockKube3Builder()
                .withPodController()
                .withDeletionController()
                .withNamespaces(NAMESPACE)
                .build();
        mockKube.start();
        client = mockKube.client();
    }

    @AfterAll
    public static void afterAll() {
        mockKube.stop();
    }

    @Test
    public void testCreateReadUpdate() {
        PodOperator pr = new PodOperator(ASYNC_EXECUTOR, client);

        pr.listAsync(NAMESPACE, Labels.EMPTY);
        TestUtils.await(pr.listAsync(NAMESPACE, Labels.EMPTY)
                .whenComplete(TestUtils::assertSuccessful)
                .thenAccept(podList -> assertThat(podList.stream().map(p -> p.getMetadata().getName()).collect(Collectors.toList()), is(List.of())))
                .thenCompose(i -> pr.createOrUpdate(new Reconciliation("test", "kind", NAMESPACE, RESOURCE_NAME), POD))
                .whenComplete(TestUtils::assertSuccessful)
                .thenCompose(i -> pr.listAsync(NAMESPACE, Labels.EMPTY))
                .whenComplete(TestUtils::assertSuccessful)
                .thenAccept(podList -> assertThat(podList.stream().map(p -> p.getMetadata().getName()).collect(Collectors.toList()), is(singletonList(RESOURCE_NAME))))
                .thenCompose(i -> pr.reconcile(new Reconciliation("test", "kind", NAMESPACE, RESOURCE_NAME), NAMESPACE, RESOURCE_NAME, null))
                .whenComplete(TestUtils::assertSuccessful));
    }
}
