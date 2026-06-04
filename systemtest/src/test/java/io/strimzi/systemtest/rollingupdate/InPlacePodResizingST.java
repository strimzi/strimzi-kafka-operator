/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.RequiredMinKubeApiVersion;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROLLING_UPDATE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(ROLLING_UPDATE)
@SuiteDoc(
        description = @Desc("Test suite for testing Kubernetes in-place resource updates (changing of Pod resources dynamically, without restart). In-place resource updates require Kubernetes 1.35 and newer."),
        beforeTestSteps = {
            @Step(value = "Deploy Cluster Operator with default installation.", expected = "Cluster Operator is deployed."),
            @Step(value = "Deploy Kafka with single mixed-role node pool and enabled in-place resizing.", expected = "Kafka cluster is deployed without any issue."),
            @Step(value = "Deploy Connect cluster with single node and enabled in-place resizing.", expected = "Connect cluster is deployed without any issue."),
        },
        labels = {
            @Label(TestDocsLabels.KAFKA)
        }
)
@RequiredMinKubeApiVersion(version = 1.35)
public class InPlacePodResizingST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(InPlacePodResizingST.class);

    private TestStorage testStorage;

    @TestDoc(
            description = @Desc("Checks the in-place resource updates for Kafka nodes."),
            steps = {
                @Step(value = "Slightly increase the resource request and limits in the `KafkaNodePool`.", expected = "The resources are updated dynamically without any rolling updates."),
            },
            labels = {
                @Label(value = TestDocsLabels.KAFKA)
            }
    )
    @IsolatedTest
    void testDynamicInPlaceKafkaResourceUpdates() {
        Map<String, String> mixedPoolPodSnapshots = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMixedPoolSelector());

        LOGGER.info("Resizing Kafka pods -> no rolling update expected");
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), pool -> pool.getSpec().setResources(new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("510m"), "memory", new Quantity("1050Mi")))
                .withLimits(Map.of("cpu", new Quantity("510m"), "memory", new Quantity("1050Mi")))
                .build()));
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getMixedPoolSelector(), mixedPoolPodSnapshots);

        LOGGER.info("Verifying that the Kafka Pods are resized");
        KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getMixedPoolSelector()).forEach(pod -> {
            assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits().get("cpu"), is(new Quantity("510m")));
            assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits().get("memory"), is(new Quantity("1050Mi")));
            assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests().get("cpu"), is(new Quantity("510m")));
            assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests().get("memory"), is(new Quantity("1050Mi")));
            assertThat(pod.getStatus().getContainerStatuses().get(0).getResources().getLimits().get("cpu"), is(new Quantity("510m")));
            assertThat(pod.getStatus().getContainerStatuses().get(0).getResources().getLimits().get("memory"), is(new Quantity("1050Mi")));
            assertThat(pod.getStatus().getContainerStatuses().get(0).getResources().getRequests().get("cpu"), is(new Quantity("510m")));
            assertThat(pod.getStatus().getContainerStatuses().get(0).getResources().getRequests().get("memory"), is(new Quantity("1050Mi")));
        });
    }

    @TestDoc(
            description = @Desc("Checks the in-place resource updates for Connect nodes."),
            steps = {
                @Step(value = "Slightly increase the resource request and limits in the `KafkaConnect` resource.", expected = "The resources are updated dynamically without any rolling updates."),
            },
            labels = {
                @Label(value = TestDocsLabels.KAFKA)
            }
    )
    @IsolatedTest
    void testDynamicInPlaceConnectResourceUpdates() {
        final Map<String, String> connectPodsSnapshot = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector());

        LOGGER.info("Resizing Connect pods -> no rolling update expected");
        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), c -> c.getSpec().setResources(new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("510m"), "memory", new Quantity("1050Mi")))
                .withLimits(Map.of("cpu", new Quantity("510m"), "memory", new Quantity("1050Mi")))
                .build()));
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), connectPodsSnapshot);

        LOGGER.info("Verifying that the Connect Pods are resized");
        KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()).forEach(pod -> {
            assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits().get("cpu"), is(new Quantity("510m")));
            assertThat(pod.getSpec().getContainers().get(0).getResources().getLimits().get("memory"), is(new Quantity("1050Mi")));
            assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests().get("cpu"), is(new Quantity("510m")));
            assertThat(pod.getSpec().getContainers().get(0).getResources().getRequests().get("memory"), is(new Quantity("1050Mi")));
            assertThat(pod.getStatus().getContainerStatuses().get(0).getResources().getLimits().get("cpu"), is(new Quantity("510m")));
            assertThat(pod.getStatus().getContainerStatuses().get(0).getResources().getLimits().get("memory"), is(new Quantity("1050Mi")));
            assertThat(pod.getStatus().getContainerStatuses().get(0).getResources().getRequests().get("cpu"), is(new Quantity("510m")));
            assertThat(pod.getStatus().getContainerStatuses().get(0).getResources().getRequests().get("memory"), is(new Quantity("1050Mi")));
        });
    }

    @TestDoc(
            description = @Desc("Checks the in-place resource updates of a Kafka cluster with infeasible amount of resources."),
            steps = {
                @Step(value = "Update resource request and limits in the `KafkaNodePool` to values higher then the total capacity of the node.", expected = "Dynamic resource update is infeasible and the Cluster Operator rolls the first broker Pod that becomes `Pending`."),
                @Step(value = "Update the resources again back to the original value.", expected = "The Cluster Operator recovers the `Pending` Kafka node.")
            },
            labels = {
                @Label(value = TestDocsLabels.KAFKA)
            }
    )
    @IsolatedTest
    void testInfeasibleInPlaceKafkaResourceUpdates() {
        AtomicReference<ResourceRequirements> originalResources = new AtomicReference<>();

        LOGGER.info("Updating the Kafka resources to Infeasible amount of CPU");
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), pool -> {
            originalResources.set(pool.getSpec().getResources());
            pool.getSpec().setResources(new ResourceRequirementsBuilder()
                    .withRequests(Map.of("cpu", new Quantity("510"), "memory", new Quantity("1050Mi")))
                    .withLimits(Map.of("cpu", new Quantity("510"), "memory", new Quantity("1050Mi")))
                    .build());
        });

        LOGGER.info("Waiting for pending Kafka Pod");
        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), KafkaComponents.getPodSetName(testStorage.getClusterName(), testStorage.getMixedPoolName()));

        LOGGER.info("Fixing the pending Kafka Pod");
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), pool -> pool.getSpec().setResources(originalResources.get()));
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getMixedPoolSelector(), 3);
    }

    @TestDoc(
            description = @Desc("Checks the in-place resource updates of a Connect cluster with infeasible amount of resources."),
            steps = {
                @Step(value = "Update resource request and limits in the `KafkaConnect` resource to values higher then the total capacity of the node.", expected = "Dynamic resource update is infeasible and the Cluster Operator rolls the first broker Pod that becomes `Pending`."),
                @Step(value = "Update the resources again back to the original value.", expected = "The Cluster Operator recovers the `Pending` Connect node.")
            },
            labels = {
                @Label(value = TestDocsLabels.KAFKA)
            }
    )
    @IsolatedTest
    void testInfeasibleInPlaceConnectResourceUpdates() {
        AtomicReference<ResourceRequirements> originalResources = new AtomicReference<>();

        LOGGER.info("Updating the Connect resources to Infeasible amount of CPU");
        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), c -> {
            originalResources.set(c.getSpec().getResources());
            c.getSpec().setResources(new ResourceRequirementsBuilder()
                    .withRequests(Map.of("cpu", new Quantity("510"), "memory", new Quantity("1050Mi")))
                    .withLimits(Map.of("cpu", new Quantity("510"), "memory", new Quantity("1050Mi")))
                    .build());
        });

        LOGGER.info("Waiting for pending Connect Pod");
        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Fixing the pending Connect Pod");
        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), c -> c.getSpec().setResources(originalResources.get()));
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 1);
    }

    @TestDoc(
            description = @Desc("Checks the in-place resource updates of a Kafka cluster with amount of resources equal to the node capacity to get deferred resizing."),
            steps = {
                @Step(value = "Update resource request and limits in the `KafkaNodePool` to values exactly at the total capacity of the node.", expected = "Dynamic resource update is deferred and the Cluster Operator rolls the first broker Pod that becomes `Pending`."),
                @Step(value = "Update the resources again back to the original value.", expected = "The Cluster Operator recovers the `Pending` Kafka node.")
            },
            labels = {
                @Label(value = TestDocsLabels.KAFKA)
            }
    )
    @IsolatedTest
    void testDeferredInPlaceKafkaResourceUpdates() {
        List<Pod> pods = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), KafkaComponents.getPodSetName(testStorage.getClusterName(), testStorage.getMixedPoolName()));
        Node worker = KubeResourceManager.get().kubeClient().getClient().nodes().withName(pods.get(0).getSpec().getNodeName()).get();

        AtomicReference<ResourceRequirements> originalResources = new AtomicReference<>();

        LOGGER.info("Updating the Kafka resources to Deferred amount of CPU");
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), pool -> {
            originalResources.set(pool.getSpec().getResources());
            pool.getSpec().setResources(new ResourceRequirementsBuilder()
                    .withRequests(Map.of("cpu", worker.getStatus().getAllocatable().get("cpu"), "memory", worker.getStatus().getAllocatable().get("memory")))
                    .withLimits(Map.of("cpu", worker.getStatus().getAllocatable().get("cpu"), "memory", worker.getStatus().getAllocatable().get("memory")))
                    .build());
        });

        LOGGER.info("Waiting for pending Kafka Pod");
        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), KafkaComponents.getPodSetName(testStorage.getClusterName(), testStorage.getMixedPoolName()));

        LOGGER.info("Fixing the pending Kafka Pod");
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), pool -> pool.getSpec().setResources(originalResources.get()));
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getMixedPoolSelector(), 3);
    }

    @TestDoc(
            description = @Desc("Checks the in-place resource updates of a Connect cluster with amount of resources equal to the node capacity to get deferred resizing."),
            steps = {
                @Step(value = "Update resource request and limits in the `KafkaConnect` resource to values exactly at the total capacity of the node.", expected = "Dynamic resource update is deferred and the Cluster Operator rolls the first broker Pod that becomes `Pending`."),
                @Step(value = "Update the resources again back to the original value.", expected = "The Cluster Operator recovers the `Pending` Connect node.")
            },
            labels = {
                @Label(value = TestDocsLabels.KAFKA)
            }
    )
    @IsolatedTest
    void testDeferredInPlaceConnectResourceUpdates() {
        List<Pod> pods = PodUtils.getPodsByPrefixInNameWithDynamicWait(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()));
        Node worker = KubeResourceManager.get().kubeClient().getClient().nodes().withName(pods.get(0).getSpec().getNodeName()).get();

        AtomicReference<ResourceRequirements> originalResources = new AtomicReference<>();

        LOGGER.info("Updating the Connect resources to Deferred amount of CPU");
        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), c -> {
            originalResources.set(c.getSpec().getResources());
            c.getSpec().setResources(new ResourceRequirementsBuilder()
                    .withRequests(Map.of("cpu", worker.getStatus().getAllocatable().get("cpu"), "memory", worker.getStatus().getAllocatable().get("memory")))
                    .withLimits(Map.of("cpu", worker.getStatus().getAllocatable().get("cpu"), "memory", worker.getStatus().getAllocatable().get("memory")))
                    .build());
        });

        LOGGER.info("Waiting for pending Connect Pod");
        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), KafkaConnectResources.componentName(testStorage.getClusterName()));

        LOGGER.info("Fixing the pending Connect Pod");
        KafkaConnectUtils.replace(testStorage.getNamespaceName(), testStorage.getClusterName(), c -> c.getSpec().setResources(originalResources.get()));
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector(), 1);
    }

    @BeforeAll
    void setup() {
        testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_SHORT)
                .build()
            )
            .install();

        // Deploy operands we will use during the tests

        final int brokerNodes = 3;
        final int connectNodes = 1;

        LOGGER.info("Deploying Kafka cluster with mixed nodes (3 replicas) and in-place resizing enabled)");

        // Create dedicated controller and broker KafkaNodePools and Kafka CR
        KubeResourceManager.get().createResourceWithWait(
                KafkaNodePoolTemplates.mixedPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), testStorage.getClusterName(), brokerNodes)
                        .editSpec()
                            .withResources(new ResourceRequirementsBuilder()
                                    .withRequests(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("1024Mi")))
                                    .withLimits(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("1024Mi")))
                                    .build())
                        .endSpec()
                        .build(),
                KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), brokerNodes)
                        .editMetadata()
                            .addToAnnotations(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, "true")
                        .endMetadata()
                        .build()
        );

        LOGGER.info("Deploying Kafka Connect cluster with 1 node and in-place resizing enabled)");

        KubeResourceManager.get().createResourceWithWait(
                KafkaConnectTemplates.kafkaConnect(testStorage.getNamespaceName(), testStorage.getClusterName(), connectNodes)
                        .editMetadata()
                            .addToAnnotations(Annotations.ANNO_STRIMZI_IO_IN_PLACE_RESIZING, "true")
                        .endMetadata()
                        .editSpec()
                            .withResources(new ResourceRequirementsBuilder()
                                    .withRequests(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("1024Mi")))
                                    .withLimits(Map.of("cpu", new Quantity("500m"), "memory", new Quantity("1024Mi")))
                                    .build())
                        .endSpec()
                        .build()
        );
    }
}
