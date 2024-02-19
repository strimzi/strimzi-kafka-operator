/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.enums.ClusterOperatorRBACType;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;

import static io.strimzi.systemtest.TestConstants.CONNECT;
import static io.strimzi.systemtest.TestConstants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
public class ClusterOperatorRbacST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorRbacST.class);

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testCRBDeletionErrorIsIgnoredWhenRackAwarenessIsNotEnabled() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        assumeFalse(Environment.isNamespaceRbacScope());

        // 060-Deployment
        this.clusterOperator = this.clusterOperator.defaultInstallation()
            .withClusterOperatorRBACType(ClusterOperatorRBACType.NAMESPACE)
            .withWatchingNamespaces(Environment.TEST_SUITE_NAMESPACE)
            .withBindingsNamespaces(Arrays.asList(TestConstants.CO_NAMESPACE, Environment.TEST_SUITE_NAMESPACE))
            .createInstallation()
            .runBundleInstallation();

        cluster.setNamespace(Environment.TEST_SUITE_NAMESPACE);

        String coPodName = kubeClient().getClusterOperatorPodName(TestConstants.CO_NAMESPACE);
        LOGGER.info("Deploying Kafka: {}, which should be deployed even the CRBs are not present", testStorage.getClusterName());

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3).build());
        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for Kafka");
        String log = cmdKubeClient().namespace(TestConstants.CO_NAMESPACE).execInCurrentNamespace(Level.DEBUG, "logs", coPodName).out();
        assertTrue(log.contains("Kafka(" + cmdKubeClient().namespace() + "/" + testStorage.getClusterName() + "): Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));

        LOGGER.info("Deploying KafkaConnect: {} without rack awareness, the CR should be deployed without error", testStorage.getClusterName());
        resourceManager.createResourceWithWait(KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, 1).build());

        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for KafkaConnect");
        log = cmdKubeClient().namespace(TestConstants.CO_NAMESPACE).execInCurrentNamespace(Level.DEBUG, "logs", coPodName).out();
        assertTrue(log.contains("KafkaConnect(" + cmdKubeClient().namespace() + "/" + testStorage.getClusterName() + "): Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testCRBDeletionErrorsWhenRackAwarenessIsEnabled() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        assumeFalse(Environment.isNamespaceRbacScope());

        // 060-Deployment
        this.clusterOperator = this.clusterOperator.defaultInstallation()
            .withClusterOperatorRBACType(ClusterOperatorRBACType.NAMESPACE)
            .withWatchingNamespaces(Environment.TEST_SUITE_NAMESPACE)
            .withBindingsNamespaces(Arrays.asList(TestConstants.CO_NAMESPACE, Environment.TEST_SUITE_NAMESPACE))
            .createInstallation()
            .runBundleInstallation();

        String rackKey = "rack-key";

        LOGGER.info("Deploying Kafka: {}, which should not be deployed and error should be present in CR status message", testStorage.getClusterName());
        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 3).build()
            )
        );
        resourceManager.createResourceWithoutWait(KafkaTemplates.kafkaEphemeral(testStorage.getClusterName(), 3, 3)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
            .editOrNewSpec()
                .editOrNewKafka()
                    .withNewRack()
                        .withTopologyKey(rackKey)
                    .endRack()
                .endKafka()
            .endSpec()
            .build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, ".*code=403.*");
        Condition kafkaStatusCondition = KafkaResource.kafkaClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().filter(con -> NotReady.toString().equals(con.getType())).findFirst().orElse(null);
        assertThat(kafkaStatusCondition, is(notNullValue()));
        assertTrue(kafkaStatusCondition.getMessage().contains("code=403"));

        resourceManager.createResourceWithoutWait(KafkaConnectTemplates.kafkaConnect(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, testStorage.getClusterName(), 1)
            .editSpec()
                .withNewRack(rackKey)
            .endSpec()
            .build());

        KafkaConnectUtils.waitUntilKafkaConnectStatusConditionContainsMessage(testStorage.getClusterName(), Environment.TEST_SUITE_NAMESPACE, ".*code=403.*");
        Condition kafkaConnectStatusCondition = KafkaConnectResource.kafkaConnectClient().inNamespace(Environment.TEST_SUITE_NAMESPACE).withName(testStorage.getClusterName()).get().getStatus().getConditions().stream().filter(con -> NotReady.toString().equals(con.getType())).findFirst().orElse(null);
        assertThat(kafkaConnectStatusCondition, is(notNullValue()));
        assertTrue(kafkaConnectStatusCondition.getMessage().contains("code=403"));
    }
}
