/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators;

import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.enums.ClusterOperatorRBACType;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.resources.ResourceManager.cmdKubeClient;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@Tag(REGRESSION)
@IsolatedSuite
public class ClusterOperatorRbacIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(ClusterOperatorRbacIsolatedST.class);

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testCRBDeletionErrorIsIgnoredWhenRackAwarenessIsNotEnabled(ExtensionContext extensionContext) {
        assumeFalse(Environment.isNamespaceRbacScope());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // 060-Deployment
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(Constants.INFRA_NAMESPACE)
            .withClusterOperatorRBACType(ClusterOperatorRBACType.NAMESPACE)
            .createInstallation()
            .runBundleInstallation();

        String coPodName = kubeClient().getClusterOperatorPodName(clusterOperator.getDeploymentNamespace());
        LOGGER.info("Deploying Kafka: {}, which should be deployed even the CRBs are not present", clusterName);

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3).build());

        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for Kafka");
        String log = cmdKubeClient().execInCurrentNamespace(Level.DEBUG, "logs", coPodName).out();
        assertTrue(log.contains("Kafka(" + cmdKubeClient().namespace() + "/" + clusterName + "): Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));

        LOGGER.info("Deploying KafkaConnect: {} without rack awareness, the CR should be deployed without error", clusterName);
        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(clusterName, clusterOperator.getDeploymentNamespace(), 1).build());

        LOGGER.info("CO log should contain some information about ignoring forbidden access to CRB for KafkaConnect");
        log = cmdKubeClient().execInCurrentNamespace(Level.DEBUG, "logs", coPodName).out();
        assertTrue(log.contains("KafkaConnect(" + cmdKubeClient().namespace() + "/" + clusterName + "): Ignoring forbidden access to ClusterRoleBindings resource which does not seem to be required."));
    }

    @IsolatedTest("We need for each test case its own Cluster Operator")
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testCRBDeletionErrorsWhenRackAwarenessIsEnabled(ExtensionContext extensionContext) {
        assumeFalse(Environment.isNamespaceRbacScope());

        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        // 060-Deployment
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(Constants.INFRA_NAMESPACE)
            .withClusterOperatorRBACType(ClusterOperatorRBACType.NAMESPACE)
            .createInstallation()
            .runBundleInstallation();

        String rackKey = "rack-key";

        LOGGER.info("Deploying Kafka: {}, which should not be deployed and error should be present in CR status message", clusterName);
        resourceManager.createResource(extensionContext, false, KafkaTemplates.kafkaEphemeral(clusterName, 3, 3)
            .editOrNewSpec()
                .editOrNewKafka()
                    .withNewRack()
                        .withTopologyKey(rackKey)
                    .endRack()
                .endKafka()
            .endSpec()
            .build());

        KafkaUtils.waitUntilKafkaStatusConditionContainsMessage(clusterName, clusterOperator.getDeploymentNamespace(), ".*Forbidden!.*");
        Condition kafkaStatusCondition = KafkaResource.kafkaClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(clusterName).get().getStatus().getConditions().stream().filter(con -> NotReady.toString().equals(con.getType())).findFirst().orElse(null);
        assertThat(kafkaStatusCondition, is(notNullValue()));
        assertTrue(kafkaStatusCondition.getMessage().contains("Configured service account doesn't have access."));

        resourceManager.createResource(extensionContext, false, KafkaConnectTemplates.kafkaConnect(clusterName, clusterOperator.getDeploymentNamespace(), clusterName, 1)
            .editSpec()
                .withNewRack(rackKey)
            .endSpec()
            .build());

        KafkaConnectUtils.waitUntilKafkaConnectStatusConditionContainsMessage(clusterName, clusterOperator.getDeploymentNamespace(), ".*Forbidden!.*");
        Condition kafkaConnectStatusCondition = KafkaConnectResource.kafkaConnectClient().inNamespace(clusterOperator.getDeploymentNamespace()).withName(clusterName).get().getStatus().getConditions().stream().filter(con -> NotReady.toString().equals(con.getType())).findFirst().orElse(null);
        assertThat(kafkaConnectStatusCondition, is(notNullValue()));
        assertTrue(kafkaConnectStatusCondition.getMessage().contains("Configured service account doesn't have access."));
    }
}
