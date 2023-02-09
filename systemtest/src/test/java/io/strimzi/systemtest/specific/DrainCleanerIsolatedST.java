/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.RequiredMinKubeApiVersion;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.draincleaner.SetupDrainCleaner;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.RollingUpdateUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NodeUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

@Tag(REGRESSION)
@IsolatedSuite
public class DrainCleanerIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DrainCleanerIsolatedST.class);
    private static SetupDrainCleaner drainCleaner = new SetupDrainCleaner();

    @Tag(ACCEPTANCE)
    @IsolatedTest
    @RequiredMinKubeApiVersion(version = 1.17)
    void testDrainCleanerWithComponents(ExtensionContext extensionContext) {
        TestStorage testStorage = new TestStorage(extensionContext, Constants.DRAIN_CLEANER_NAMESPACE);

        final int replicas = 3;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), replicas)
            .editMetadata()
                .withNamespace(Constants.DRAIN_CLEANER_NAMESPACE)
            .endMetadata()
            .editSpec()
                .editKafka()
                    .editOrNewTemplate()
                        .editOrNewPodDisruptionBudget()
                            .withMaxUnavailable(0)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endKafka()
                .editZookeeper()
                    .editOrNewTemplate()
                        .editOrNewPodDisruptionBudget()
                            .withMaxUnavailable(0)
                        .endPodDisruptionBudget()
                    .endTemplate()
                .endZookeeper()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName())
            .editMetadata()
                .withNamespace(Constants.DRAIN_CLEANER_NAMESPACE)
            .endMetadata()
            .build());
        drainCleaner.createDrainCleaner(extensionContext);

        KafkaClients kafkaBasicExampleClients = new KafkaClientsBuilder()
            .withMessageCount(300)
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(Constants.DRAIN_CLEANER_NAMESPACE)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withDelayMs(1000)
            .build();

        resourceManager.createResource(extensionContext,
            kafkaBasicExampleClients.producerStrimzi(),
            kafkaBasicExampleClients.consumerStrimzi());

        for (int i = 0; i < replicas; i++) {
            String zkPodName = KafkaResources.zookeeperPodName(testStorage.getClusterName(), i);
            String kafkaPodName = KafkaResources.kafkaPodName(testStorage.getClusterName(), i);

            Map<String, String> zkPod = null;
            if (!Environment.isKRaftModeEnabled()) {
                zkPod = PodUtils.podSnapshot(Constants.DRAIN_CLEANER_NAMESPACE, testStorage.getZookeeperSelector()).entrySet()
                        .stream().filter(snapshot -> snapshot.getKey().equals(zkPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            Map<String, String> kafkaPod = PodUtils.podSnapshot(Constants.DRAIN_CLEANER_NAMESPACE, testStorage.getKafkaSelector()).entrySet()
                .stream().filter(snapshot -> snapshot.getKey().equals(kafkaPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!Environment.isKRaftModeEnabled()) {
                LOGGER.info("Evicting pods: {}", zkPodName);
                kubeClient().getClient().pods().inNamespace(Constants.DRAIN_CLEANER_NAMESPACE).withName(zkPodName).evict();
            }
            LOGGER.info("Evicting pods: {}", kafkaPodName);
            kubeClient().getClient().pods().inNamespace(Constants.DRAIN_CLEANER_NAMESPACE).withName(kafkaPodName).evict();

            if (!Environment.isKRaftModeEnabled()) {
                RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(Constants.DRAIN_CLEANER_NAMESPACE, testStorage.getZookeeperSelector(), replicas, zkPod);
            }
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(Constants.DRAIN_CLEANER_NAMESPACE, testStorage.getKafkaSelector(), replicas, kafkaPod);
        }

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), Constants.DRAIN_CLEANER_NAMESPACE, 300);
    }

    @AfterEach
    void teardown() {
        kubeClient().getClusterNodes().forEach(node -> NodeUtils.cordonNode(node.getMetadata().getName(), true));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(Constants.DRAIN_CLEANER_NAMESPACE)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_DEFAULT)
            .createInstallation()
            .runInstallation();
    }
}
