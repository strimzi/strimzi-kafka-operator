/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.enums.ClusterOperatorRBACType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.junit.jupiter.api.Tag;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
class NamespaceRbacScopeOperatorST extends AbstractST {

    @IsolatedTest("This test case needs own Cluster Operator")
    void testNamespacedRbacScopeDeploysRoles() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        assumeFalse(Environment.isOlmInstall() || Environment.isHelmInstall());

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withClusterOperatorRBACType(ClusterOperatorRBACType.NAMESPACE)
                .withNamespacesToWatch(TestConstants.CO_NAMESPACE + "," + Environment.TEST_SUITE_NAMESPACE)
                .build()
            )
            .install();

        resourceManager.createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editMetadata()
                .addToLabels("app", "strimzi")
            .endMetadata()
            .build());

        // Wait for Kafka to be Ready to ensure all potentially erroneous ClusterRole applications have happened
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName());

        // Assert that no ClusterRoles are present on the server that have app strimzi
        // Naturally returns false positives if another Strimzi operator has been installed
        List<ClusterRole> strimziClusterRoles = kubeClient().listClusterRoles().stream()
            .filter(cr -> {
                Map<String, String> labels = cr.getMetadata().getLabels() != null ? cr.getMetadata().getLabels() : Collections.emptyMap();
                return "strimzi".equals(labels.get("app"));
            })
            .collect(Collectors.toList());
        assertThat(strimziClusterRoles, is(Collections.emptyList()));
    }
}
