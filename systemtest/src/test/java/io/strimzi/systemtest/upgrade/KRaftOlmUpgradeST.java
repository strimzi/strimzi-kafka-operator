/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfiguration;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.openshift.SubscriptionTemplates;
import io.strimzi.systemtest.upgrade.VersionModificationDataLoader.ModificationType;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.OlmUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static io.strimzi.systemtest.TestConstants.CO_NAMESPACE;
import static io.strimzi.systemtest.TestTags.OLM_UPGRADE;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;


/**
 * This test class contains tests for Strimzi downgrade from version X to version X - 1.
 * The difference between this class and {@link KRaftStrimziUpgradeST} is in cluster operator install type.
 * Tests in this class use OLM for install cluster operator.
 */
@Tag(OLM_UPGRADE)
public class KRaftOlmUpgradeST extends AbstractKRaftUpgradeST {

    private static final Logger LOGGER = LogManager.getLogger(KRaftOlmUpgradeST.class);
    private final OlmVersionModificationData olmUpgradeData = new VersionModificationDataLoader(ModificationType.OLM_UPGRADE).getOlmUpgradeData();

    @IsolatedTest
    void testStrimziUpgrade() throws IOException {
        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext(), CO_NAMESPACE);
        final String fromVersion = olmUpgradeData.getFromVersion();
        ClusterOperatorConfiguration clusterOperatorConfiguration = new ClusterOperatorConfigurationBuilder()
            .withNamespaceName(CO_NAMESPACE)
            .withNamespacesToWatch(CO_NAMESPACE)
            .withOlmChannelName(olmUpgradeData.getFromOlmChannelName())
            .withOlmOperatorVersion(fromVersion)
            .withOlmInstallationStrategy(OlmInstallationStrategy.Manual)
            .build();

        LOGGER.info("====================================================================================");
        LOGGER.info("---------------- Updating Cluster Operator version " + fromVersion + " => HEAD -----------------");
        LOGGER.info("====================================================================================");
        LOGGER.info("-------------------------- Upgrade data used in this test --------------------------");
        LOGGER.info(olmUpgradeData.toString());
        LOGGER.info("====================================================================================");

        // Install operator via Olm
        //  1. Create subscription with manual approval and operator group if needed
        //  2. Approve installation (get install-plan and patch it)
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(clusterOperatorConfiguration)
            .installUsingOlm();

        // In this test we intend to setup Kafka once at the beginning and then upgrade it with CO
        File dir = FileUtils.downloadAndUnzip(olmUpgradeData.getFromUrl());
        File kafkaYaml = new File(dir, olmUpgradeData.getFromExamples() + olmUpgradeData.getKafkaFilePathBefore());

        LOGGER.info("Deploying Kafka in Namespace: {} from file: {}", CO_NAMESPACE, kafkaYaml.getPath());
        KubeClusterResource.cmdKubeClient(CO_NAMESPACE).create(kafkaYaml);
        waitForReadinessOfKafkaCluster(CO_NAMESPACE);

        // Create KafkaTopic
        final String topicUpgradeName = "topic-upgrade";
        HashMap<String, Object> topicConfig = new HashMap<String, Object>();
        topicConfig.put("min.insync.replicas", 2);

        KafkaTopic kafkaUpgradeTopic = new YAMLMapper().readValue(new File(dir, olmUpgradeData.getFromExamples() + "/examples/topic/kafka-topic.yaml"), KafkaTopic.class);
        kafkaUpgradeTopic = new KafkaTopicBuilder(kafkaUpgradeTopic)
            .editMetadata()
                .withNamespace(CO_NAMESPACE)
                .withName(topicUpgradeName)
            .endMetadata()
            .editSpec()
                .withReplicas(3)
                .addToConfig(topicConfig)
            .endSpec()
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaUpgradeTopic);

        KafkaClients kafkaBasicClientJob = new KafkaClientsBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(CO_NAMESPACE)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
            .withTopicName(topicUpgradeName)
            .withMessageCount(testStorage.getMessageCount())
            .withDelayMs(1000)
            .build();

        KubeResourceManager.get().createResourceWithWait(kafkaBasicClientJob.producerStrimzi(), kafkaBasicClientJob.consumerStrimzi());

        clusterOperatorConfiguration.setOperatorDeploymentName(kubeClient().namespace(CO_NAMESPACE).getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME));
        LOGGER.info("Old deployment name of Cluster Operator is {}", clusterOperatorConfiguration.getOperatorDeploymentName());

        // ======== Cluster Operator upgrade starts ========
        makeComponentsSnapshots(CO_NAMESPACE);

        clusterOperatorConfiguration = new ClusterOperatorConfigurationBuilder(clusterOperatorConfiguration)
            .withOlmChannelName("stable")
            .build();

        // Perform upgrade of the Cluster Operator
        upgradeClusterOperator(clusterOperatorConfiguration);

        LOGGER.info("New deployment name of Cluster Operator is {}", clusterOperatorConfiguration.getOperatorDeploymentName());

        // Verification that Cluster Operator has been upgraded to a correct version
        String afterUpgradeVersionOfCo = kubeClient().getCsvWithPrefix(CO_NAMESPACE, clusterOperatorConfiguration.getOlmAppBundlePrefix()).getSpec().getVersion();
        assertThat(afterUpgradeVersionOfCo, is(not(fromVersion)));

        // Wait for Rolling Update to finish
        controllerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(CO_NAMESPACE, controllerSelector, 3, controllerPods);
        brokerPods = RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(CO_NAMESPACE, brokerSelector, 3, brokerPods);
        eoPods = DeploymentUtils.waitTillDepHasRolled(CO_NAMESPACE, KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1, eoPods);
        // ======== Cluster Operator upgrade ends ========

        // ======== Kafka upgrade starts ========
        logComponentsPodImages(CO_NAMESPACE);
        changeKafkaVersion(CO_NAMESPACE, olmUpgradeData);
        logComponentsPodImages(CO_NAMESPACE);
        // ======== Kafka upgrade ends ========

        // Wait for messages of previously created clients
        ClientUtils.waitForClientsSuccess(CO_NAMESPACE, testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    private void upgradeClusterOperator(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        updateSubscription(clusterOperatorConfiguration);

        // Because we are updating to latest available CO, we want to wait for new install plan with CSV prefix, not with the exact CSV (containing the prefix and version)
        OlmUtils.waitForNonApprovedInstallPlanWithCsvNameOrPrefix(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOlmAppBundlePrefix());
        String newDeploymentName = OlmUtils.approveNonApprovedInstallPlanAndReturnDeploymentName(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOlmAppBundlePrefix());

        clusterOperatorConfiguration.setOperatorDeploymentName(kubeClient().getDeploymentNameByPrefix(clusterOperatorConfiguration.getNamespaceName(), newDeploymentName));
        DeploymentUtils.waitForCreationOfDeploymentWithPrefix(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOperatorDeploymentName());

        DeploymentUtils.waitForDeploymentAndPodsReady(clusterOperatorConfiguration.getNamespaceName(), clusterOperatorConfiguration.getOperatorDeploymentName(), 1);
    }

    private void updateSubscription(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        Subscription subscription = SubscriptionTemplates.clusterOperatorSubscription(clusterOperatorConfiguration);
        KubeResourceManager.get().updateResource(subscription);
    }
}
