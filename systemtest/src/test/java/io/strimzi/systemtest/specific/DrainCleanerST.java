/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.MultiNodeClusterOnly;
import io.strimzi.systemtest.annotations.RequiredMinKubeApiVersion;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.resources.draincleaner.SetupDrainCleaner;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.StatefulSetUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.NodeUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.resources.ResourceManager.kubeClient;

@Tag(REGRESSION)
public class DrainCleanerST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(DrainCleanerST.class);
    private static SetupDrainCleaner drainCleaner = new SetupDrainCleaner();

    @IsolatedTest
    @RequiredMinKubeApiVersion(version = 1.17)
    void testDrainCleanerWithComponents(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String producerName = clusterName + "-producer";
        final String consumerName = clusterName + "-consumer";
        final int replicas = 3;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, replicas)
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

        resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName).build());
        drainCleaner.createDrainCleaner(extensionContext);

        String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        String zkName = KafkaResources.zookeeperStatefulSetName(clusterName);

        KafkaBasicExampleClients kafkaBasicExampleClients = new KafkaBasicExampleClients.Builder()
            .withMessageCount(300)
            .withTopicName(topicName)
            .withNamespaceName(Constants.DRAIN_CLEANER_NAMESPACE)
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
            .withProducerName(producerName)
            .withConsumerName(consumerName)
            .withDelayMs(1000)
            .build();

        resourceManager.createResource(extensionContext,
            kafkaBasicExampleClients.producerStrimzi().build(),
            kafkaBasicExampleClients.consumerStrimzi().build());

        for (int i = 0; i < replicas; i++) {
            String zkPodName = KafkaResources.zookeeperPodName(clusterName, i);
            String kafkaPodName = KafkaResources.kafkaPodName(clusterName, i);

            Map<String, String> kafkaPod = StatefulSetUtils.ssSnapshot(Constants.DRAIN_CLEANER_NAMESPACE, kafkaName).entrySet()
                .stream().filter(snapshot -> snapshot.getKey().equals(kafkaPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            Map<String, String> zkPod = StatefulSetUtils.ssSnapshot(Constants.DRAIN_CLEANER_NAMESPACE, zkName).entrySet()
                .stream().filter(snapshot -> snapshot.getKey().equals(zkPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            LOGGER.info("Evicting pods: {} and {}", zkPodName, kafkaPodName);
            kubeClient().getClient().pods().inNamespace(Constants.DRAIN_CLEANER_NAMESPACE).withName(zkPodName).evict();
            kubeClient().getClient().pods().inNamespace(Constants.DRAIN_CLEANER_NAMESPACE).withName(kafkaPodName).evict();

            StatefulSetUtils.waitTillSsHasRolled(Constants.DRAIN_CLEANER_NAMESPACE, zkName, replicas, zkPod);
            StatefulSetUtils.waitTillSsHasRolled(Constants.DRAIN_CLEANER_NAMESPACE, kafkaName, replicas, kafkaPod);
        }

        ClientUtils.waitTillContinuousClientsFinish(producerName, consumerName, Constants.DRAIN_CLEANER_NAMESPACE, 300);
    }

    @IsolatedTest
    @MultiNodeClusterOnly
    void testDrainCleanerWithComponentsDuringNodeDraining(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String topicName = mapWithTestTopics.get(extensionContext.getDisplayName());
        final String producerName = clusterName + "-producer";
        final String consumerName = clusterName + "-consumer";
        String rackKey = "rack-key";
        final int replicas = 3;

        int size = 5;

        List<String> topicNames = IntStream.range(0, size).boxed().map(i -> topicName + "-" + i).collect(Collectors.toList());
        List<String> producerNames = IntStream.range(0, size).boxed().map(i -> producerName + "-" + i).collect(Collectors.toList());
        List<String> consumerNames = IntStream.range(0, size).boxed().map(i -> consumerName + "-" + i).collect(Collectors.toList());
        List<String> continuousConsumerGroups = IntStream.range(0, size).boxed().map(i -> "continuous-consumer-group-" + i).collect(Collectors.toList());

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, replicas)
            .editSpec()
                .editKafka()
                    .withNewRack()
                        .withTopologyKey(rackKey)
                    .endRack()
                    .editOrNewTemplate()
                        .editOrNewPodDisruptionBudget()
                            .withMaxUnavailable(0)
                        .endPodDisruptionBudget()
                        .withNewPod()
                            .withAffinity(
                                new AffinityBuilder()
                                    .withNewPodAntiAffinity()
                                        .addNewRequiredDuringSchedulingIgnoredDuringExecution()
                                            .editOrNewLabelSelector()
                                                .addNewMatchExpression()
                                                    .withKey(rackKey)
                                                    .withOperator("In")
                                                    .withValues("zone")
                                                .endMatchExpression()
                                            .endLabelSelector()
                                        .withTopologyKey(rackKey)
                                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                                    .endPodAntiAffinity()
                                    .build())
                        .endPod()
                    .endTemplate()
                .endKafka()
                .editZookeeper()
                    .editOrNewTemplate()
                        .editOrNewPodDisruptionBudget()
                            .withMaxUnavailable(0)
                        .endPodDisruptionBudget()
                        .withNewPod()
                            .withAffinity(
                                new AffinityBuilder()
                                    .withNewPodAntiAffinity()
                                        .addNewRequiredDuringSchedulingIgnoredDuringExecution()
                                            .editOrNewLabelSelector()
                                                .addNewMatchExpression()
                                                    .withKey(rackKey)
                                                    .withOperator("In")
                                                    .withValues("zone")
                                                .endMatchExpression()
                                            .endLabelSelector()
                                            .withTopologyKey(rackKey)
                                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                                    .endPodAntiAffinity()
                                    .build())
                        .endPod()
                    .endTemplate()
                .endZookeeper()
            .endSpec()
            .build());

        topicNames.forEach(topic -> resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topic, 3, 3, 2).build()));
        drainCleaner.createDrainCleaner(extensionContext);

        String kafkaName = KafkaResources.kafkaStatefulSetName(clusterName);
        String zkName = KafkaResources.zookeeperStatefulSetName(clusterName);

        Map<String, List<String>> nodesWithPods = NodeUtils.getPodsForEachNodeInNamespace(Constants.DRAIN_CLEANER_NAMESPACE);
        // remove all pods from map, which doesn't contain "kafka" or "zookeeper" in its name
        nodesWithPods.forEach(
            (node, podlist) -> podlist.retainAll(podlist.stream().filter(podName -> (podName.contains("kafka") || podName.contains("zookeeper"))).collect(Collectors.toList()))
        );

        String producerAdditionConfiguration = "delivery.timeout.ms=30000\nrequest.timeout.ms=30000";
        KafkaBasicExampleClients kafkaBasicExampleClients;

        for (int i = 0; i < size; i++) {
            kafkaBasicExampleClients = new KafkaBasicExampleClients.Builder()
                .withProducerName(producerNames.get(i))
                .withConsumerName(consumerNames.get(i))
                .withTopicName(topicNames.get(i))
                .withConsumerGroup(continuousConsumerGroups.get(i))
                .withMessageCount(300)
                .withNamespaceName(Constants.DRAIN_CLEANER_NAMESPACE)
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withDelayMs(1000)
                .withAdditionalConfig(producerAdditionConfiguration)
                .build();

            resourceManager.createResource(extensionContext,
                kafkaBasicExampleClients.producerStrimzi().build(),
                kafkaBasicExampleClients.consumerStrimzi().build());
        }

        LOGGER.info("Starting Node drain");

        nodesWithPods.forEach((nodeName, podList) -> {
            String zkPodName = podList.stream().filter(podName -> podName.contains("zookeeper")).findFirst().get();
            String kafkaPodName = podList.stream().filter(podName -> podName.contains("kafka")).findFirst().get();

            Map<String, String> kafkaPod = StatefulSetUtils.ssSnapshot(Constants.DRAIN_CLEANER_NAMESPACE, kafkaName).entrySet()
                .stream().filter(snapshot -> snapshot.getKey().equals(kafkaPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            Map<String, String> zkPod = StatefulSetUtils.ssSnapshot(Constants.DRAIN_CLEANER_NAMESPACE, zkName).entrySet()
                .stream().filter(snapshot -> snapshot.getKey().equals(zkPodName)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            NodeUtils.drainNode(nodeName);
            NodeUtils.cordonNode(nodeName, true);

            StatefulSetUtils.waitTillSsHasRolled(Constants.DRAIN_CLEANER_NAMESPACE, zkName, replicas, zkPod);
            StatefulSetUtils.waitTillSsHasRolled(Constants.DRAIN_CLEANER_NAMESPACE, kafkaName, replicas, kafkaPod);
        });

        producerNames.forEach(producer -> ClientUtils.waitTillContinuousClientsFinish(producer, consumerNames.get(producerNames.indexOf(producer)), Constants.DRAIN_CLEANER_NAMESPACE, 300));
        producerNames.forEach(producer -> KubeClusterResource.kubeClient().deleteJob(producer));
        consumerNames.forEach(consumer -> KubeClusterResource.kubeClient().deleteJob(consumer));
    }

    @AfterEach
    void teardown(ExtensionContext extensionContext) throws Exception {
        drainCleaner.teardownDrainCleaner();
        kubeClient().getClusterNodes().forEach(node -> NodeUtils.cordonNode(node.getMetadata().getName(), true));
        super.afterEachMayOverride(extensionContext);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        install = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(extensionContext)
            .withNamespace(Constants.DRAIN_CLEANER_NAMESPACE)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_DEFAULT)
            .createInstallation()
            .runInstallation();
    }
}
