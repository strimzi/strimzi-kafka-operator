/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.rollingupdate;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.RequiredMinKubeApiVersion;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaNodePoolUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.TestTags.ROLLING_UPDATE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(ROLLING_UPDATE)
public class InPlacePodResizingST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(InPlacePodResizingST.class);

    @ParallelNamespaceTest
    @RequiredMinKubeApiVersion(version = 1.35)
    void testInPlaceResourceUpdates() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        final int brokerNodes = 3;

        LOGGER.info("Deploying Kafka cluster with mixed nodes (3 replicas) and in-place resizing enabled)");

        // Create dedicated controller and broker KafkaNodePools and Kafka CR
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.mixedPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), testStorage.getClusterName(), 3)
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

        Map<String, String> mixedPoolPodSnapshots = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getMixedPoolSelector());

        LOGGER.info("Resizing pods -> no rolling update expected");
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), pool -> pool.getSpec().setResources(new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("510m"), "memory", new Quantity("1050Mi")))
                .withLimits(Map.of("cpu", new Quantity("510m"), "memory", new Quantity("1050Mi")))
                .build()));
        RollingUpdateUtils.waitForNoRollingUpdate(testStorage.getNamespaceName(), testStorage.getMixedPoolSelector(), mixedPoolPodSnapshots);

        LOGGER.info("Verifying that the Pods are resized");
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

        LOGGER.info("Updating the resources to Infeasible amount of CPU");
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), pool -> pool.getSpec().setResources(new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("510"), "memory", new Quantity("1050Mi")))
                .withLimits(Map.of("cpu", new Quantity("510"), "memory", new Quantity("1050Mi")))
                .build()));

        LOGGER.info("Waiting for pending Pod");
        PodUtils.waitForPendingPod(testStorage.getNamespaceName(), KafkaComponents.getPodSetName(testStorage.getClusterName(), testStorage.getMixedPoolName()));

        LOGGER.info("Fixing the pending Pod");
        KafkaNodePoolUtils.replace(testStorage.getNamespaceName(), testStorage.getMixedPoolName(), pool -> pool.getSpec().setResources(new ResourceRequirementsBuilder()
                .withRequests(Map.of("cpu", new Quantity("510m"), "memory", new Quantity("1050Mi")))
                .withLimits(Map.of("cpu", new Quantity("510m"), "memory", new Quantity("1050Mi")))
                .build()));
        RollingUpdateUtils.waitForComponentAndPodsReady(testStorage.getNamespaceName(), testStorage.getMixedPoolSelector(), 3);
    }

    @BeforeAll
    void setup() {
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_MEDIUM)
                .build()
            )
            .install();
    }
}
