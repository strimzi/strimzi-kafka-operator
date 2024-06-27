/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class PodOperatorServerSideApplyApiServerTest {
    public static final String RESOURCE_NAME = "my-resource";
    public static final String NAMESPACE = "podoperatorserversideapplyapiservertest";

    private final static PodBuilder STARTING_POD = new PodBuilder()
            .withNewSpec()
            .withHostname("foo")
            .withContainers(new ContainerBuilder().withName("my-container").withImage("alpine").build())
            .endSpec();

    protected static Vertx vertx;
    private static WorkerExecutor sharedWorkerExecutor;
    private static KubernetesClient client;
    private static MockKube3 mockKube;

    @BeforeAll
    public static void before() {
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaConnectorCrd()
                .build();
        mockKube.start();
        client = mockKube.client();

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
        mockKube.prepareNamespace(NAMESPACE);
    }

    @BeforeEach
    public void beforeEach() {
        client.pods()
                .inNamespace(NAMESPACE)
                .withTimeoutInMillis(5000)
                .delete();
    }

    @AfterAll
    public static void after() {
        sharedWorkerExecutor.close();
        vertx.close();
        mockKube.stop();
    }

    @Test
    public void testShouldNotRemoveAnnotationsFromExistingPod(VertxTestContext context) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool", 10);
        PodOperator pr = new PodOperator(vertx, client, true);

        String name = RESOURCE_NAME;
        Pod startingPod = STARTING_POD.withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
                .withAnnotations(Map.of("test-annotation", "test-value")).endMetadata().build();
        client.pods().inNamespace(NAMESPACE).resource(startingPod).create();

        Pod createdPod = STARTING_POD.withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
                .withAnnotations(Map.of("new-annotation", "test-value")).endMetadata().build();

        Map<String, Object> expectedAnnotations = new HashMap<>();
        expectedAnnotations.putAll(startingPod.getMetadata().getAnnotations());
        expectedAnnotations.putAll(createdPod.getMetadata().getAnnotations());

        Checkpoint async = context.checkpoint(1);
        pr.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, createdPod).onComplete(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));
            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                        .map(p -> p.getMetadata().getName())
                        .collect(Collectors.toList()), hasItem(name)));

            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                    .filter(pod -> pod.getMetadata().getName().equals(name))
                    .map(p -> p.getMetadata().getAnnotations())
                    .collect(Collectors.toList()), is(singletonList(expectedAnnotations))));
            async.flag();
        });
    }

    @Test
    public void testShouldForceFieldsToBeManagedByStrimzi(VertxTestContext context) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool", 10);
        PodOperator pr = new PodOperator(vertx, client, true);

        String name = RESOURCE_NAME;

        Pod startingPod = STARTING_POD.withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
                .withAnnotations(Map.of("test-annotation", "test-value")).endMetadata().build();
        client.pods().inNamespace(NAMESPACE).resource(startingPod).create();

        List<ManagedFieldsEntry> managedFields = pr.list(NAMESPACE, Labels.EMPTY).stream()
                .filter(pod -> pod.getMetadata().getName().equals(name))
                .flatMap(p -> p.getMetadata().getManagedFields().stream())
                .filter(entry -> entry.getManager().equals("fabric8-kubernetes-client"))
                .toList();

        assertThat(managedFields.isEmpty(), is(false));

        Pod createdPod = STARTING_POD.withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
                .withAnnotations(Map.of("test-annotation", "a-new-value")).endMetadata().build();

        Map<String, Object> expectedAnnotations = new HashMap<>();
        expectedAnnotations.putAll(createdPod.getMetadata().getAnnotations());

        Checkpoint async = context.checkpoint(1);
        pr.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, createdPod).onComplete(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));
            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                        .map(p -> p.getMetadata().getName())
                        .collect(Collectors.toList()), hasItem(name)));

            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                    .filter(pod -> pod.getMetadata().getName().equals(name))
                    .map(p -> p.getMetadata().getAnnotations())
                    .collect(Collectors.toList()), is(singletonList(expectedAnnotations))));

            //assert that strimzi manages the new annotation
            context.verify(() -> {
                var foundManagedFields = pr.list(NAMESPACE, Labels.EMPTY).stream()
                        .filter(pod -> pod.getMetadata().getName().equals(name))
                        .flatMap(p -> p.getMetadata().getManagedFields().stream())
                        .toList();
                assertThat(annotationManagedBy("test-annotation", "strimzi-cluster-operator", foundManagedFields), is(true));
            });

            async.flag();
        });
    }

    private boolean annotationManagedBy(String annotationName, String owner, List<ManagedFieldsEntry> managedFieldsEntries) {
        return managedFieldsEntries.stream()
                .filter(managedFieldsEntry -> managedFieldsEntry.getManager().equals(owner))
                .anyMatch(managedFieldsEntry -> {
                    var properties = managedFieldsEntry.getFieldsV1().getAdditionalProperties();
                    Map<String, Object> metadata = (Map<String, Object>) properties.get("f:metadata");
                    Map<String, Object> annotations = (Map<String, Object>) metadata.get("f:annotations");
                    return annotations.containsKey("f:" + annotationName);
                });
    }

    @Test
    public void testShouldRemoveAnnotationsFromPreviousPod(VertxTestContext context) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool", 10);
        PodOperator pr = new PodOperator(vertx, client, true);

        String name = RESOURCE_NAME;
        Pod startingPod = STARTING_POD.withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
                .withAnnotations(Map.of("existing-annotation", "test-value")).endMetadata().build();
        client.pods().inNamespace(NAMESPACE).resource(startingPod).create();

        Pod createdPod = STARTING_POD.withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
                .withAnnotations(Map.of("created-annotation", "test-value")).endMetadata().build();

        Pod updatedPod = STARTING_POD.withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
                .withAnnotations(Map.of("updated-annotation", "test-value")).endMetadata().build();

        Map<String, Object> expectedAnnotationsAfterUpdate = new HashMap<>();
        expectedAnnotationsAfterUpdate.putAll(startingPod.getMetadata().getAnnotations());
        expectedAnnotationsAfterUpdate.putAll(createdPod.getMetadata().getAnnotations());

        Map<String, Object> expectedAnnotationsAfterReconcile = new HashMap<>();
        expectedAnnotationsAfterReconcile.putAll(startingPod.getMetadata().getAnnotations());
        expectedAnnotationsAfterReconcile.putAll(updatedPod.getMetadata().getAnnotations());

        Checkpoint async = context.checkpoint(1);
        pr.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, createdPod).onComplete(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));
            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                        .map(p -> p.getMetadata().getName())
                        .collect(Collectors.toList()), hasItem(name)));

            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                    .filter(pod -> pod.getMetadata().getName().equals(name))
                    .map(p -> p.getMetadata().getAnnotations())
                    .collect(Collectors.toList()), is(singletonList(expectedAnnotationsAfterUpdate))));


            pr.reconcile(Reconciliation.DUMMY_RECONCILIATION, NAMESPACE, name, updatedPod).onComplete(updatedResult -> {
                context.verify(() -> assertThat(updatedResult.succeeded(), is(true)));

                context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                        .filter(pod -> pod.getMetadata().getName().equals(name))
                        .map(p -> p.getMetadata().getAnnotations())
                        .collect(Collectors.toList()), is(singletonList(expectedAnnotationsAfterReconcile))));

                async.flag();
            });
        });
    }

    @Test
    public void testShouldCreatePodWhenOneDoesNotExist(VertxTestContext context) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool", 10);
        PodOperator pr = new PodOperator(vertx, client, true);

        String name = RESOURCE_NAME;
        Pod createdPod = STARTING_POD.withNewMetadata()
                .withNamespace(NAMESPACE)
                .withName(name)
                .withAnnotations(Map.of("created-annotation", "test-value")).endMetadata().build();

        Map<String, Object> expectedAnnotations = new HashMap<>();
        expectedAnnotations.putAll(createdPod.getMetadata().getAnnotations());

        Checkpoint async = context.checkpoint(1);
        pr.createOrUpdate(Reconciliation.DUMMY_RECONCILIATION, createdPod).onComplete(createResult -> {
            context.verify(() -> assertThat(createResult.succeeded(), is(true)));
            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                    .map(p -> p.getMetadata().getName())
                    .collect(Collectors.toList()), hasItem(name)));

            context.verify(() -> assertThat(pr.list(NAMESPACE, Labels.EMPTY).stream()
                    .filter(pod -> pod.getMetadata().getName().equals(name))
                    .map(p -> p.getMetadata().getAnnotations())
                    .collect(Collectors.toList()), is(singletonList(expectedAnnotations))));
            async.flag();
        });
    }
}
