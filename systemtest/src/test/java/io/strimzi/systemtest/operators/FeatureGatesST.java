/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;

import java.util.Map;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Feature Gates should give us additional options on
 * how to control and mature different behaviors in the operators.
 * https://github.com/strimzi/proposals/blob/main/022-feature-gates.md
 */
@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Feature Gates test suite verifying that feature gates provide additional options to control operator behavior, specifically testing Server Side Apply functionality."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with configurable feature gates.", expected = "Cluster Operator is deployed with feature gate support.")
    },
    labels = {
        @Label(value = TestDocsLabels.KAFKA)
    }
)
public class FeatureGatesST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(FeatureGatesST.class);

    @IsolatedTest("Enables UseBackgroundPodDeletion feature gate in CO")
    @TestDoc(
        description = @Desc("This test verifies that the UseBackgroundPodDeletion feature gate works correctly. When enabled, Kafka broker pods are deleted with BACKGROUND propagation during rolling restarts, and the rolling restart completes successfully."),
        steps = {
            @Step(value = "Deploy Cluster Operator with UseBackgroundPodDeletion enabled.", expected = "Cluster Operator is deployed with background pod deletion feature gate."),
            @Step(value = "Create Kafka cluster with broker and controller node pools.", expected = "Kafka cluster is deployed and ready."),
            @Step(value = "Trigger manual rolling update of broker pods.", expected = "Rolling update is triggered via manual-rolling-update annotation."),
            @Step(value = "Wait for broker pods to finish rolling.", expected = "All broker pods are rolled and ready, confirming background deletion works correctly.")
        },
        labels = {
            @Label(value = TestDocsLabels.KAFKA)
        }
    )
    void testUseBackgroundPodDeletion() {
        TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        LOGGER.info("Deploying CO with UseBackgroundPodDeletion enabled");
        // by default, the UseBackgroundPodDeletion feature gate is enabled
        setupClusterOperatorWithFeatureGate("");

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());

        Map<String, String> brokerPods = PodUtils.podSnapshot(testStorage.getNamespaceName(), testStorage.getBrokerSelector());

        LOGGER.info("Triggering manual rolling update of broker pods");
        // annotating Pods and not StrimziPodSet to not hit race condition when applying the manual rolling update annotation
        for (String brokerPod : brokerPods.keySet()) {
            PodUtils.annotatePod(testStorage.getNamespaceName(), brokerPod, Annotations.ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE, "true");
        }

        brokerPods = RollingUpdateUtils.waitTillComponentHasRolled(testStorage.getNamespaceName(), testStorage.getBrokerSelector(), 3, brokerPods);

        assertThat("Broker pods were rolled successfully with UseBackgroundPodDeletion enabled", brokerPods.size(), is(3));
    }

    /**
     * Sets up a Cluster Operator with specified feature gates.
     *
     * @param extraFeatureGates A String representing additional feature gates (comma separated) to be
     *                          enabled or disabled for the Cluster Operator.
     */
    private void setupClusterOperatorWithFeatureGate(String extraFeatureGates) {
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withFeatureGates(extraFeatureGates)
                // configuring it explicitly here to be sure that we will use the correct reconciliation interval
                .withReconciliationInterval(TestConstants.RECONCILIATION_INTERVAL)
                .build()
            )
            .install();
    }
}
