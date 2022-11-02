/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.StrimziPodSetList;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.PodSetUtils;
import io.strimzi.operator.cluster.operator.resource.PodRevision;
import io.strimzi.operator.common.AbstractOperator;
import io.strimzi.operator.common.MetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.StrimziPodSetOperator;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube2.MockKube2;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(VertxExtension.class)
public class StrimziPodSetControllerMockTest {
    private static final String NAMESPACE = "strimzi-pod-set-controller-test";
    private static final String KAFKA_NAME = "foo";
    private static final Map<String, String> MATCHING_LABELS = Map.of("selector", "matching");
    private static final String OTHER_KAFKA_NAME = "bar";
    private static final Map<String, String> OTHER_LABELS = Map.of("selector", "not-matching");

    // Injected by Fabric8 Mock Kubernetes Server
    @SuppressWarnings("unused")
    private KubernetesClient client;
    private MockKube2 mockKube;
    private Vertx vertx;
    private StrimziPodSetController controller;
    private CrdOperator<KubernetesClient, Kafka, KafkaList> kafkaOperator;
    private StrimziPodSetOperator podSetOperator;
    private PodOperator podOperator;
    private MetricsProvider metricsProvider;
    private WorkerExecutor sharedWorkerExecutor;

    @BeforeEach
    public void beforeEach() {
        // Configure the Kubernetes Mock
        mockKube = new MockKube2.MockKube2Builder(client)
                .withKafkaCrd()
                .withStrimziPodSetCrd()
                .withPodController()
                .build();
        mockKube.start();

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");
        kafkaOperator = new CrdOperator<>(vertx, client, Kafka.class, KafkaList.class, Kafka.RESOURCE_KIND);
        podSetOperator = new StrimziPodSetOperator(vertx, client, 10_000L);
        podOperator = new PodOperator(vertx, client);
        metricsProvider = ResourceUtils.metricsProvider();

        kafkaOp().inNamespace(NAMESPACE).resource(kafka(KAFKA_NAME, MATCHING_LABELS)).create();
        kafkaOp().inNamespace(NAMESPACE).resource(kafka(OTHER_KAFKA_NAME, OTHER_LABELS)).create();

        startController();
    }

    @AfterEach
    public void afterEach() {
        stopController();
        mockKube.stop();
        sharedWorkerExecutor.close();
        vertx.close();
    }

    /*
     * Util methods
     */

    private MixedOperation<Kafka, KafkaList, Resource<Kafka>> kafkaOp() {
        return client.resources(Kafka.class, KafkaList.class);
    }

    private static Kafka kafka(String name, Map<String, String> labels)   {
        return new KafkaBuilder()
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(NAMESPACE)
                        .withLabels(labels)
                    .endMetadata()
                    .withNewSpec()
                        .withNewKafka()
                            .withReplicas(3)
                            .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build())
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endKafka()
                        .withNewZookeeper()
                            .withReplicas(3)
                            .withNewEphemeralStorage()
                            .endEphemeralStorage()
                        .endZookeeper()
                    .endSpec()
                    .build();
    }

    private MixedOperation<StrimziPodSet, StrimziPodSetList, Resource<StrimziPodSet>> podSetOp() {
        return client.resources(StrimziPodSet.class, StrimziPodSetList.class);
    }

    private static StrimziPodSet podSet(String name, String kafkaName, Pod... pods)   {
        return new StrimziPodSetBuilder()
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(NAMESPACE)
                        .withLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, kafkaName))
                    .endMetadata()
                    .withNewSpec()
                        .withSelector(new LabelSelector(null, Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, kafkaName)))
                        .withPods(PodSetUtils.podsToMaps(Arrays.asList(pods)))
                    .endSpec()
                    .build();
    }

    private static Pod pod(String name, String kafkaName, String podSetName)    {
        Pod pod = new PodBuilder()
                    .withNewMetadata()
                        .withName(name)
                        .withNamespace(NAMESPACE)
                        .withLabels(Map.of(Labels.STRIMZI_KIND_LABEL, "Kafka", Labels.STRIMZI_CLUSTER_LABEL, kafkaName, Labels.STRIMZI_NAME_LABEL, podSetName))
                        .withAnnotations(new HashMap<>())
                    .endMetadata()
                    .withNewSpec()
                        .withContainers(new ContainerBuilder()
                                .withName("busybox")
                                .withImage("quay.io/scholzj/busybox:latest") // Quay.io is used to avoid Docker Hub limits
                                .withCommand("sleep", "3600")
                                .withImagePullPolicy("IfNotPresent")
                                .build())
                        .withRestartPolicy("Always")
                        .withTerminationGracePeriodSeconds(0L)
                    .endSpec()
                    .build();

        pod.getMetadata().getAnnotations().put(PodRevision.STRIMZI_REVISION_ANNOTATION, PodRevision.getRevision(Reconciliation.DUMMY_RECONCILIATION, pod));

        return pod;
    }

    private static void checkOwnerReference(HasMetadata resource, String podSetName)  {
        OwnerReference owner = resource
                .getMetadata()
                .getOwnerReferences()
                .stream()
                .filter(o -> "StrimziPodSet".equals(o.getKind()))
                .findFirst()
                .orElse(null);

        assertThat(owner, is(notNullValue()));
        assertThat(owner.getKind(), is("StrimziPodSet"));
        assertThat(owner.getApiVersion(), is(StrimziPodSet.RESOURCE_GROUP + "/" + StrimziPodSet.V1BETA2));
        assertThat(owner.getName(), is(podSetName));
    }

    private void startController()  {
        controller = new StrimziPodSetController(NAMESPACE, Labels.fromMap(MATCHING_LABELS), kafkaOperator, podSetOperator, podOperator, metricsProvider, ClusterOperatorConfig.DEFAULT_POD_SET_CONTROLLER_WORK_QUEUE_SIZE);
        controller.start();
    }

    private void stopController()   {
        controller.stop();
    }

    /*
     * Tests
     */

    /**
     * Tests the basic operations:
     *   - Creation of StrimziPodSet and the managed pod
     *   - Re-creation of the managed pod when it is deleted
     *   - Deletion of the StrimziPodSet and the managed pod
     *
     * @param context   Test context
     */
    @Test
    public void testPodCreationDeletionAndRecreation(VertxTestContext context) {
        String podSetName = "basic-test";
        String podName = podSetName + "-0";

        try {
            Pod pod = pod(podName, KAFKA_NAME, podSetName);
            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, pod)).create();

            // Check that pod is created
            TestUtils.waitFor(
                    "Wait for Pod to be created",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(podName).get() != null,
                    () -> context.failNow("Test timed out waiting for pod creation!"));

            // Wait until the pod is ready
            TestUtils.waitFor(
                    "Wait for Pod to be ready",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(podName).isReady(),
                    () -> context.failNow("Test timed out waiting for pod readiness!"));

            Pod actualPod = client.pods().inNamespace(NAMESPACE).withName(podName).get();

            // Check OwnerReference was added
            checkOwnerReference(actualPod, podSetName);

            // We keep the resource version for pod re-creation test
            String resourceVersion = actualPod.getMetadata().getResourceVersion();

            // Check status of the PodSet
            TestUtils.waitFor(
                    "Wait for StrimziPodSetStatus",
                    100,
                    10_000,
                    () -> {
                        StrimziPodSet podSet = podSetOp().inNamespace(NAMESPACE).withName(podSetName).get();
                        return podSet.getStatus().getCurrentPods() == 1
                                && podSet.getStatus().getReadyPods() == 1
                                && podSet.getStatus().getPods() == 1;
                    },
                    () -> context.failNow("Pod stats do not match"));

            // Delete the pod and test that it is recreated
            client.pods().inNamespace(NAMESPACE).withName(podName).delete();

            // Check that pod is created
            TestUtils.waitFor(
                    "Wait for Pod to be recreated",
                    100,
                    10_000,
                    () -> {
                        Pod p = client.pods().inNamespace(NAMESPACE).withName(podName).get();
                        return p != null && !resourceVersion.equals(p.getMetadata().getResourceVersion());
                    },
                    () -> context.failNow("Test timed out waiting for pod recreation!"));

            context.completeNow();
        } finally {
            podSetOp().inNamespace(NAMESPACE).withName(podSetName).delete();
        }
    }

    /**
     * Tests scaling up and down of the StrimziPodSet and updates of the StrimziPodSet status.
     *
     * @param context   Test context
     */
    @Test
    public void testScaleUpScaleDown(VertxTestContext context) {
        String podSetName = "scale-up-down";
        String pod1Name = podSetName + "-0";
        String pod2Name = podSetName + "-1";

        try {
            Pod pod1 = pod(pod1Name, KAFKA_NAME, podSetName);
            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, pod1)).create();

            // Wait until the pod is ready
            TestUtils.waitFor(
                    "Wait for Pod to be ready",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(pod1Name).isReady(),
                    () -> context.failNow("Test timed out waiting for pod readiness!"));

            // Check status of the PodSet
            TestUtils.waitFor(
                    "Wait for StrimziPodSetStatus",
                    100,
                    10_000,
                    () -> {
                        StrimziPodSet podSet = podSetOp().inNamespace(NAMESPACE).withName(podSetName).get();
                        return podSet.getStatus().getCurrentPods() == 1
                                && podSet.getStatus().getReadyPods() == 1
                                && podSet.getStatus().getPods() == 1;
                    },
                    () -> context.failNow("Pod stats do not match"));

            // Scale-up the pod-set
            Pod pod2 = pod(pod2Name, KAFKA_NAME, podSetName);
            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, pod1, pod2)).replace();

            // Wait until the new pod is ready
            TestUtils.waitFor(
                    "Wait for second Pod to be ready",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(pod2Name).isReady(),
                    () -> context.failNow("Test timed out waiting for second pod readiness!"));

            // Check status of the PodSet
            TestUtils.waitFor(
                    "Wait for StrimziPodSetStatus",
                    100,
                    10_000,
                    () -> {
                        StrimziPodSet podSet = podSetOp().inNamespace(NAMESPACE).withName(podSetName).get();
                        return podSet.getStatus().getCurrentPods() == 2
                                && podSet.getStatus().getReadyPods() == 2
                                && podSet.getStatus().getPods() == 2;
                    },
                    () -> context.failNow("Pod stats do not match"));

            // Scale-down the pod-set
            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, pod1)).replace();

            // Wait until the pod is deleted
            TestUtils.waitFor(
                    "Wait for second Pod to be deleted",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(pod2Name).get() == null,
                    () -> context.failNow("Test timed out waiting for second pod to be deleted!"));

            // Check status of the PodSet
            TestUtils.waitFor(
                    "Wait for StrimziPodSetStatus",
                    100,
                    10_000,
                    () -> {
                        StrimziPodSet podSet = podSetOp().inNamespace(NAMESPACE).withName(podSetName).get();
                        return podSet.getStatus().getCurrentPods() == 1
                                && podSet.getStatus().getReadyPods() == 1
                                && podSet.getStatus().getPods() == 1;
                    },
                    () -> context.failNow("Pod stats do not match"));

            context.completeNow();
        } finally {
            podSetOp().inNamespace(NAMESPACE).withName(podSetName).delete();
        }
    }

    /**
     * Tests updates pods in the StrimziPodSet:
     *   - StrimziPodSetController should not roll the pods => the dedicated rollers do it
     *   - The pod should not be marked as current when it is updated
     *
     * @param context   Test context
     */
    @Test
    public void testPodUpdates(VertxTestContext context) {
        String podSetName = "pod-updates";
        String podName = podSetName + "-0";

        try {
            Pod originalPod = pod(podName, KAFKA_NAME, podSetName);
            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, originalPod)).create();

            // Wait until the pod is ready
            TestUtils.waitFor(
                    "Wait for Pod to be ready",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(podName).isReady(),
                    () -> context.failNow("Test timed out waiting for pod readiness!"));

            // Check status of the PodSet
            TestUtils.waitFor(
                    "Wait for StrimziPodSetStatus",
                    100,
                    10_000,
                    () -> {
                        StrimziPodSet podSet = podSetOp().inNamespace(NAMESPACE).withName(podSetName).get();
                        return podSet.getStatus().getCurrentPods() == 1
                                && podSet.getStatus().getReadyPods() == 1
                                && podSet.getStatus().getPods() == 1;
                    },
                    () -> context.failNow("Pod stats do not match"));

            // Get resource version to double-check the pod was not deleted
            Pod initialPod = client.pods().inNamespace(NAMESPACE).withName(podName).get();
            String resourceVersion = initialPod.getMetadata().getResourceVersion();

            // Update the pod with a new revision and
            Pod updatedPod = pod(podName, KAFKA_NAME, podSetName);
            updatedPod.getMetadata().getAnnotations().put(PodRevision.STRIMZI_REVISION_ANNOTATION, "new-revision");
            updatedPod.getSpec().setTerminationGracePeriodSeconds(1L);
            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, updatedPod)).replace();

            // Check status of the PodSet
            TestUtils.waitFor(
                    "Wait for StrimziPodSetStatus",
                    100,
                    10_000,
                    () -> {
                        StrimziPodSet podSet = podSetOp().inNamespace(NAMESPACE).withName(podSetName).get();
                        return podSet.getStatus().getCurrentPods() == 0
                                && podSet.getStatus().getReadyPods() == 1
                                && podSet.getStatus().getPods() == 1;
                    },
                    () -> context.failNow("Pod stats do not match"));

            // Check the pod was not changed
            Pod actualPod = client.pods().inNamespace(NAMESPACE).withName(podName).get();
            assertThat(actualPod.getMetadata().getResourceVersion(), is(resourceVersion));
            assertThat(actualPod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION), is(originalPod.getMetadata().getAnnotations().get(PodRevision.STRIMZI_REVISION_ANNOTATION)));
            assertThat(actualPod.getSpec().getTerminationGracePeriodSeconds(), is(0L));

            context.completeNow();
        } finally {
            podSetOp().inNamespace(NAMESPACE).withName(podSetName).delete();
        }
    }

    /**
     * Tests patching of the owner reference in pre-existing pods
     *
     * @param context   Test context
     */
    @Test
    public void testOwnerReferencePatching(VertxTestContext context) {
        String podSetName = "owner-reference";
        String podName = podSetName + "-0";

        try {
            Pod pod = pod(podName, KAFKA_NAME, podSetName);
            client.pods().inNamespace(NAMESPACE).resource(pod).create();

            // Wait until the pod is ready
            TestUtils.waitFor(
                    "Wait for Pod to be ready",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(podName).isReady(),
                    () -> context.failNow("Test timed out waiting for pod readiness!"));

            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, pod)).create();

            // Check status of the PodSet
            TestUtils.waitFor(
                    "Wait for StrimziPodSetStatus",
                    100,
                    10_000,
                    () -> {
                        StrimziPodSet podSet = podSetOp().inNamespace(NAMESPACE).withName(podSetName).get();
                        return podSet.getStatus().getCurrentPods() == 1
                                && podSet.getStatus().getReadyPods() == 1
                                && podSet.getStatus().getPods() == 1;
                    },
                    () -> context.failNow("Pod stats do not match"));

            // Get the pod and check that the owner reference was set
            Pod actualPod = client.pods().inNamespace(NAMESPACE).withName(podName).get();
            checkOwnerReference(actualPod, podSetName);

            context.completeNow();
        } finally {
            podSetOp().inNamespace(NAMESPACE).withName(podSetName).delete();
        }
    }

    /**
     * Tests that the controller will ignore pods or node sets when the Kafka cluster they belong to doesn't match the
     * custom resource selector
     *
     * @param context   Test context
     */
    @Test
    public void testCrSelector(VertxTestContext context) {
        String podSetName = "matching-podset";
        String otherPodSetName = "other-podset";
        String podName = podSetName + "-0";
        String preExistingPodName = podSetName + "-1";
        String otherPodName = otherPodSetName + "-0";
        String otherPreExistingPodName = otherPodSetName + "-1";

        try {
            // Create the pod set which should be reconciled
            Pod pod = pod(podName, KAFKA_NAME, podSetName);
            Pod preExistingPod = pod(preExistingPodName, KAFKA_NAME, podSetName);
            client.pods().inNamespace(NAMESPACE).resource(preExistingPod).create();
            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, pod)).create();

            // Create the pod set which should be ignored
            Pod otherPod = pod(otherPodName, OTHER_KAFKA_NAME, otherPodSetName);
            Pod otherPreExistingPod = pod(otherPreExistingPodName, OTHER_KAFKA_NAME, otherPodSetName);
            client.pods().inNamespace(NAMESPACE).resource(otherPreExistingPod).create();
            podSetOp().inNamespace(NAMESPACE).resource(podSet(otherPodSetName, OTHER_KAFKA_NAME, otherPod)).create();

            // Check that the pre-existing pod for matching pod set is deleted
            TestUtils.waitFor(
                    "Wait for the pre-existing Pod to be deleted",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(preExistingPodName).get() == null,
                    () -> context.failNow("Test timed out waiting for pod deletion!"));

            // Check that the pod for matching pod set is ready
            TestUtils.waitFor(
                    "Wait for Pod to be ready",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(podName).isReady(),
                    () -> context.failNow("Test timed out waiting for pod readiness!"));

            // Check status of the matching pod set which should be updated
            TestUtils.waitFor(
                    "Wait for StrimziPodSetStatus",
                    100,
                    10_000,
                    () -> {
                        StrimziPodSet podSet = podSetOp().inNamespace(NAMESPACE).withName(podSetName).get();
                        return podSet.getStatus().getCurrentPods() == 1
                                && podSet.getStatus().getReadyPods() == 1
                                && podSet.getStatus().getPods() == 1;
                    },
                    () -> context.failNow("Pod stats do not match"));

            // Check that the non-matching pod set was ignored
            assertThat(client.pods().inNamespace(NAMESPACE).withName(otherPodName).get(), is(nullValue()));
            assertThat(podSetOp().inNamespace(NAMESPACE).withName(otherPodSetName).get().getStatus(), is(nullValue()));
            assertThat(client.pods().inNamespace(NAMESPACE).withName(otherPreExistingPodName).get(), is(notNullValue()));

            context.completeNow();
        } finally {
            podSetOp().inNamespace(NAMESPACE).withName(podSetName).delete();
            podSetOp().inNamespace(NAMESPACE).withName(otherPodSetName).delete();
            client.pods().inNamespace(NAMESPACE).withName(otherPreExistingPodName).delete();
            client.pods().inNamespace(NAMESPACE).withName(preExistingPodName).delete();
        }
    }

    /**
     * Tests the metrics during the reconciliation
     *   - It creates and deletes the SPS
     *   - Checks the metrics during these operations
     *
     * @param context   Test context
     */
    @Test
    public void testMetrics(VertxTestContext context) {
        String podSetName = "metrics-test";
        String podName = podSetName + "-0";

        try {
            Pod pod = pod(podName, KAFKA_NAME, podSetName);
            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, pod)).create();

            // Wait for PodSet to be ready
            TestUtils.waitFor(
                    "Wait for StrimziPodSetStatus",
                    100,
                    10_000,
                    () -> {
                        StrimziPodSet podSet = podSetOp().inNamespace(NAMESPACE).withName(podSetName).get();
                        return podSet.getStatus().getCurrentPods() == 1
                                && podSet.getStatus().getReadyPods() == 1
                                && podSet.getStatus().getPods() == 1;
                    },
                    () -> context.failNow("Pod stats do not match"));

            // Check the metrics
            // Depending on timing, there might be multiple reconciliations happening. That is why we use of greaterThanOrEqualTo
            MeterRegistry registry = metricsProvider.meterRegistry();

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resources").meter().getId().getTags(), contains(Tag.of("kind", "StrimziPodSet"), Tag.of("namespace", "strimzi-pod-set-controller-test"), Tag.of("selector", "selector=matching")));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "resources").tag("kind", "StrimziPodSet").gauge().value(), is(1.0));

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").meter().getId().getTags(), contains(Tag.of("kind", "StrimziPodSet"), Tag.of("namespace", "strimzi-pod-set-controller-test"), Tag.of("selector", "selector=matching")));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations").tag("kind", "StrimziPodSet").counter().count(), greaterThanOrEqualTo(3.0));

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").meter().getId().getTags(), contains(Tag.of("kind", "StrimziPodSet"), Tag.of("namespace", "strimzi-pod-set-controller-test"), Tag.of("selector", "selector=matching")));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.successful").tag("kind", "StrimziPodSet").counter().count(), greaterThanOrEqualTo(3.0));

            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").meter().getId().getTags(), contains(Tag.of("kind", "StrimziPodSet"), Tag.of("namespace", "strimzi-pod-set-controller-test"), Tag.of("selector", "selector=matching")));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "StrimziPodSet").timer().count(), greaterThanOrEqualTo(3L));
            assertThat(registry.get(AbstractOperator.METRICS_PREFIX + "reconciliations.duration").tag("kind", "StrimziPodSet").timer().totalTime(TimeUnit.MILLISECONDS), greaterThanOrEqualTo(0.0));

            // Delete the PodSet
            podSetOp().inNamespace(NAMESPACE).withName(podSetName).delete();

            // The controller needs to react to the event => we wait until the metric is actually reset to 0 to avoid race condition
            TestUtils.waitFor(
                    "Wait for resource metric to be 0 again",
                    100,
                    1_000,
                    () -> registry.get(AbstractOperator.METRICS_PREFIX + "resources").tag("kind", "StrimziPodSet").gauge().value() == 0.0,
                    () -> context.failNow("Resource metric did not returned to 0"));

            context.completeNow();
        } finally {
            // Delete the PodSet (in case something failed)
            podSetOp().inNamespace(NAMESPACE).withName(podSetName).delete();

        }
    }

    /**
     * Tests the handling of failed Pod:
     *   - Creation of StrimziPodSet and the managed pod
     *   - Re-creation of a Pod which is moved to the Failed phase
     *
     * @param context   Test context
     */
    @Test
    public void testFailedPodRecovery(VertxTestContext context) {
        String podSetName = "basic-test";
        String podName = podSetName + "-0";

        try {
            Pod pod = pod(podName, KAFKA_NAME, podSetName);
            podSetOp().inNamespace(NAMESPACE).resource(podSet(podSetName, KAFKA_NAME, pod)).create();

            // Check that pod is created
            TestUtils.waitFor(
                    "Wait for Pod to be created",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(podName).get() != null,
                    () -> context.failNow("Test timed out waiting for pod creation!"));

            // Wait until the pod is ready
            TestUtils.waitFor(
                    "Wait for Pod to be ready",
                    100,
                    10_000,
                    () -> client.pods().inNamespace(NAMESPACE).withName(podName).isReady(),
                    () -> context.failNow("Test timed out waiting for pod readiness!"));

            Pod actualPod = client.pods().inNamespace(NAMESPACE).withName(podName).get();

            // Set the Pod phase as failed
            actualPod.getStatus().setPhase("Failed");
            Pod failedPod = client.pods().inNamespace(NAMESPACE).resource(actualPod).patchStatus();

            // We keep the resource version for pod re-creation test
            String resourceVersion = failedPod.getMetadata().getResourceVersion();

            // Check that pod is created
            TestUtils.waitFor(
                    "Wait for Pod to be recreated",
                    100,
                    10_000,
                    () -> {
                        Pod p = client.pods().inNamespace(NAMESPACE).withName(podName).get();
                        // Waits for the Pod to be re-created by the StrimziPodSetController and for its status to be
                        // updated by MockKube (and its MockPodController). Waiting for the status of the Pod to be
                        // updated is important to avoid any Null Pointer Exceptions in the asserts done after
                        // the wait is complete
                        return p != null
                                && !resourceVersion.equals(p.getMetadata().getResourceVersion())
                                && p.getStatus() != null;
                    },
                    () -> context.failNow("Test timed out waiting for pod recreation!"));

            // Check the Pod is not failed anymore
            Pod recreatedPod = client.pods().inNamespace(NAMESPACE).withName(podName).get();
            assertThat(recreatedPod.getStatus().getPhase(), is(not("Failed")));

            context.completeNow();
        } finally {
            podSetOp().inNamespace(NAMESPACE).withName(podSetName).delete();
        }
    }
}
