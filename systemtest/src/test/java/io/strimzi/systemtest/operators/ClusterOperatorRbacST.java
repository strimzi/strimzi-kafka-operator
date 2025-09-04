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
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.enums.ClusterOperatorRBACType;
import io.strimzi.systemtest.resources.CrdClients;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;

import static io.strimzi.systemtest.TestTags.CONNECT;
import static io.strimzi.systemtest.TestTags.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestTags.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite containing Cluster Operator RBAC scenarios related to ClusterRoleBinding (CRB) permissions, ensuring proper functionality when namespace-scoped RBAC is used instead of cluster-wide permissions."),
    labels = {
        @Label(value = TestDocsLabels.CONNECT)
    }
)
public class ClusterOperatorRbacST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorRbacST.class);

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @TestDoc(
        description = @Desc("This test verifies that Cluster Operator can deploy Kafka and KafkaConnect resources even when ClusterRoleBinding permissions are not available, as long as rack awareness is not enabled. The test ensures that forbidden access to CRB resources is properly ignored when not required."),
        steps = {
            @Step(value = "Deploy Cluster Operator with namespace-scoped RBAC configuration.", expected = "Cluster Operator is configured with limited ClusterRoleBinding access."),
            @Step(value = "Create KafkaNodePools for brokers and controllers without rack awareness.", expected = "KafkaNodePools are created successfully."),
            @Step(value = "Deploy Kafka cluster without rack awareness configuration.", expected = "Kafka cluster is deployed successfully despite missing CRB permissions."),
            @Step(value = "Verify Cluster Operator logs contain information about ignoring forbidden CRB access for Kafka.", expected = "Log contains message about ignoring forbidden access to ClusterRoleBindings for Kafka."),
            @Step(value = "Deploy KafkaConnect without rack awareness configuration.", expected = "KafkaConnect is deployed successfully despite missing CRB permissions."),
            @Step(value = "Verify Cluster Operator logs contain information about ignoring forbidden CRB access for KafkaConnect.", expected = "Log contains message about ignoring forbidden access to ClusterRoleBindings for KafkaConnect.")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testCRBDeletionErrorIsIgnoredWhenRackAwarenessIsNotEnabled() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        assumeFalse(Environment.isNamespaceRbacScope());

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withClusterOperatorRBACType(ClusterOperatorRBACType.NAMESPACE)
                .withNamespacesToWatch(TestConstants.CO_NAMESPACE + "," + Environment.TEST_SUITE_NAMESPACE)
                .build()
            )
            .install();

        cluster.setNamespace(Environment.TEST_SUITE_NAMESPACE);

        String coPodName = KubeResourceManager.get().kubeClient()
            .listPodsByPrefixInName(TestConstants.CO_NAMESPACE, SetupClusterOperator.getInstance().getOperatorDeploymentName()).get(0).getMetadata().getName();
        LOGGER.info("Deploying Kafka: {}, which should be deployed even the CRBs are not present", testStorage.getClusterName());

        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3).build());
        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for Kafka");
        String log = KubeResourceManager.get().kubeCmdClient().inNamespace(TestConstants.CO_NAMESPACE).logs(coPodName);
        assertTrue(log.contains("Kafka(" + Environment.TEST_SUITE_NAMESPACE + "/" + testStorage.getClusterName() + "): Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));

        LOGGER.info("Deploying KafkaConnect: {} without rack awareness, the CR should be deployed without error", testStorage.getClusterName());
        KubeResourceManager.get().createResourceWithWait(KafkaConnectTemplates.kafkaConnect(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), 1).build());

        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for KafkaConnect");
        log = KubeResourceManager.get().kubeCmdClient().inNamespace(TestConstants.CO_NAMESPACE).logs(coPodName);
        assertTrue(log.contains("KafkaConnect(" + Environment.TEST_SUITE_NAMESPACE + "/" + testStorage.getClusterName() + "): Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @TestDoc(
        description = @Desc("This test verifies that Cluster Operator fails to deploy Kafka and KafkaConnect resources when ClusterRoleBinding permissions are not available and rack awareness is enabled. The test ensures that proper error conditions are reported when CRB access is required but not available."),
        steps = {
            @Step(value = "Deploy Cluster Operator with namespace-scoped RBAC configuration.", expected = "Cluster Operator is configured with limited ClusterRoleBinding access."),
            @Step(value = "Create KafkaNodePools for brokers and controllers.", expected = "KafkaNodePools are created successfully."),
            @Step(value = "Deploy Kafka cluster with rack awareness configuration enabled.", expected = "Kafka deployment fails due to missing CRB permissions."),
            @Step(value = "Verify Kafka status condition contains 403 forbidden error message.", expected = "Kafka status shows NotReady condition with code=403 error."),
            @Step(value = "Deploy KafkaConnect with rack awareness configuration enabled.", expected = "KafkaConnect deployment fails due to missing CRB permissions."),
            @Step(value = "Verify KafkaConnect status condition contains 403 forbidden error message.", expected = "KafkaConnect status shows NotReady condition with code=403 error.")
        },
        labels = {
            @Label(value = TestDocsLabels.CONNECT)
        }
    )
    void testCRBDeletionErrorsWhenRackAwarenessIsEnabled() {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());
        assumeFalse(Environment.isNamespaceRbacScope());

        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withClusterOperatorRBACType(ClusterOperatorRBACType.NAMESPACE)
                .withNamespacesToWatch(TestConstants.CO_NAMESPACE + "," + Environment.TEST_SUITE_NAMESPACE)
                .build()
            )
            .install();

        String rackKey = "rack-key";

        LOGGER.info("Deploying Kafka: {}, which should not be deployed and error should be present in CR status message", testStorage.getClusterName());
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
            KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
        );
        KubeResourceManager.get().createResourceWithoutWait(KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 3)
            .editOrNewSpec()
                .editOrNewKafka()
                    .withNewRack()
                        .withTopologyKey(rackKey)
                    .endRack()
                .endKafka()
            .endSpec()
            .build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), ".*code=403.*");
        Condition kafkaStatusCondition = CrdClients.kafkaClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().filter(con -> NotReady.toString().equals(con.getType())).findFirst().orElse(null);
        assertThat(kafkaStatusCondition, is(notNullValue()));
        assertTrue(kafkaStatusCondition.getMessage().contains("code=403"));

        KubeResourceManager.get().createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), testStorage.getClusterName(), 1)
            .editSpec()
                .withNewRack(rackKey)
            .endSpec()
            .build());

        KafkaConnectUtils.waitUntilKafkaConnectStatusConditionContainsMessage(Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), ".*code=403.*");
        Condition kafkaConnectStatusCondition = CrdClients.kafkaConnectClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().filter(con -> NotReady.toString().equals(con.getType())).findFirst().orElse(null);
        assertThat(kafkaConnectStatusCondition, is(notNullValue()));
        assertTrue(kafkaConnectStatusCondition.getMessage().contains("code=403"));
    }
}
