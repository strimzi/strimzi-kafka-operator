/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.client.KubernetesClientException;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.MicroShiftNotSupported;
import io.strimzi.systemtest.annotations.RequiredMinKubeApiVersion;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.draincleaner.SetupDrainCleaner;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestConstants.ACCEPTANCE;
import static io.strimzi.systemtest.TestConstants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

@Tag(REGRESSION)
@MicroShiftNotSupported
public class DrainCleanerST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DrainCleanerST.class);
    private static SetupDrainCleaner drainCleaner = new SetupDrainCleaner();

    @Tag(ACCEPTANCE)
    @IsolatedTest
    @RequiredMinKubeApiVersion(version = 1.17)
    void testDrainCleanerWithComponents() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext(), TestConstants.DRAIN_CLEANER_NAMESPACE);

        final int replicas = 3;

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), replicas).build(),
                KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), replicas).build()
            )
        );
        Kafka kafka = KafkaTemplates.kafkaPersistent(testStorage.getClusterName(), replicas)
            .editMetadata()
                .withNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE)
            .endMetadata()
            .build();

        if (Environment.isKRaftModeEnabled()) {
            kafka.getSpec().setZookeeper(null);
        }

        resourceManager.createResourceWithWait(kafka);
        resourceManager.createResourceWithWait(KafkaTopicTemplates.topic(testStorage.getClusterName(), testStorage.getTopicName(), TestConstants.DRAIN_CLEANER_NAMESPACE).build());
        drainCleaner.createDrainCleaner();

        KafkaClients kafkaBasicExampleClients = new KafkaClientsBuilder()
            .withMessageCount(300)
            .withTopicName(testStorage.getTopicName())
            .withNamespaceName(TestConstants.DRAIN_CLEANER_NAMESPACE)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withDelayMs(1000)
            .build();

        resourceManager.createResourceWithWait(
            kafkaBasicExampleClients.producerStrimzi(),
            kafkaBasicExampleClients.consumerStrimzi());

        List<String> brokerPods = kubeClient().listPodNames(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getBrokerSelector());

        for (int i = 0; i < replicas; i++) {
            String zkPodName = KafkaResources.zookeeperPodName(testStorage.getClusterName(), i);
            String kafkaPodName = brokerPods.get(i);

            Map<String, String> zkPod = null;
            if (!Environment.isKRaftModeEnabled()) {
                zkPod = PodUtils.podSnapshot(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getControllerSelector()).entrySet()
                        .stream().filter(snapshot -> snapshot.getKey().equals(zkPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }
            Map<String, String> kafkaPod = PodUtils.podSnapshot(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getBrokerSelector()).entrySet()
                .stream().filter(snapshot -> snapshot.getKey().equals(kafkaPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            if (!Environment.isKRaftModeEnabled()) {
                LOGGER.info("Evicting Pods: {}", zkPodName);

                try {
                    kubeClient().getClient().pods().inNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE).withName(zkPodName).evict();
                } catch (KubernetesClientException e)   {
                    if (e.getCode() == 500 && e.getMessage().contains("The pod will be rolled by the Strimzi Cluster Operator"))    {
                        LOGGER.info("Eviction request for pod {} was denied by the Drain Cleaner", zkPodName);
                    } else {
                        throw e;
                    }
                }
            }

            LOGGER.info("Evicting Pods: {}", kafkaPodName);

            try {
                kubeClient().getClient().pods().inNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE).withName(kafkaPodName).evict();
            } catch (KubernetesClientException e)   {
                if (e.getCode() == 500 && e.getMessage().contains("The pod will be rolled by the Strimzi Cluster Operator"))    {
                    LOGGER.info("Eviction request for pod {} was denied by the Drain Cleaner", kafkaPodName);
                } else {
                    throw e;
                }
            }

            if (!Environment.isKRaftModeEnabled()) {
                RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getControllerSelector(), replicas, zkPod);
            }
            RollingUpdateUtils.waitTillComponentHasRolledAndPodsReady(TestConstants.DRAIN_CLEANER_NAMESPACE, testStorage.getBrokerSelector(), replicas, kafkaPod);
        }

        ClientUtils.waitForClientsSuccess(testStorage.getProducerName(), testStorage.getConsumerName(), TestConstants.DRAIN_CLEANER_NAMESPACE, 300);
    }

    @AfterEach
    void teardown() {
        kubeClient().getClusterNodes().forEach(node -> NodeUtils.cordonNode(node.getMetadata().getName(), true));
    }

    @BeforeAll
    void setup() {
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(ResourceManager.getTestContext())
            .withNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE)
            .withOperationTimeout(TestConstants.CO_OPERATION_TIMEOUT_DEFAULT)
            .createInstallation()
            .runInstallation();
    }
}
