/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.specific;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.MultiNodeClusterOnly;
import io.strimzi.systemtest.annotations.RequiredMinKubeApiVersion;
import io.strimzi.systemtest.resources.crd.kafkaclients.KafkaBasicExampleClients;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.strimzi.systemtest.Constants.SPECIFIC;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@Tag(SPECIFIC)
public class ClusterOperationST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(ClusterOperationST.class);

    public static final String NAMESPACE = "cluster-operations-test";

    @IsolatedTest
    @MultiNodeClusterOnly
    @RequiredMinKubeApiVersion(version = 1.15)
    void testAvailabilityDuringNodeDrain(ExtensionContext extensionContext) {
        String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());

        int size = 5;
        List<String> topicNames = IntStream.range(0, size).boxed().map(i -> "test-topic-" + i).collect(Collectors.toList());
        List<String> producerNames = IntStream.range(0, size).boxed().map(i -> "hello-world-producer-" + i).collect(Collectors.toList());
        List<String> consumerNames = IntStream.range(0, size).boxed().map(i -> "hello-world-consumer-" + i).collect(Collectors.toList());
        List<String> continuousConsumerGroups = IntStream.range(0, size).boxed().map(i -> "continuous-consumer-group-" + i).collect(Collectors.toList());
        int continuousClientsMessageCount = 300;

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaPersistent(clusterName, 3, 3)
                .editOrNewSpec()
                    .editEntityOperator()
                        .editUserOperator()
                            .withReconciliationIntervalSeconds(30)
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build());

        topicNames.forEach(topicName -> resourceManager.createResource(extensionContext, KafkaTopicTemplates.topic(clusterName, topicName, 3, 3, 2).build()));

        String producerAdditionConfiguration = "delivery.timeout.ms=20000\nrequest.timeout.ms=20000";
        KafkaBasicExampleClients kafkaBasicClientResource;

        for (int i = 0; i < size; i++) {
            kafkaBasicClientResource = new KafkaBasicExampleClients.Builder()
                .withProducerName(producerNames.get(i))
                .withConsumerName(consumerNames.get(i))
                .withBootstrapAddress(KafkaResources.plainBootstrapAddress(clusterName))
                .withTopicName(topicNames.get(i))
                .withMessageCount(continuousClientsMessageCount)
                .withAdditionalConfig(producerAdditionConfiguration)
                .withConsumerGroup(continuousConsumerGroups.get(i))
                .withDelayMs(1000)
                .build();

            resourceManager.createResource(extensionContext, kafkaBasicClientResource.producerStrimzi().build());
            resourceManager.createResource(extensionContext, kafkaBasicClientResource.consumerStrimzi().build());
        }

        // ##############################
        // Nodes draining
        // ##############################
        kubeClient().getClusterWorkers().forEach(node -> {
            drainNode(node.getMetadata().getName());
            setNodeSchedule(node.getMetadata().getName(), true);
        });

        producerNames.forEach(producerName -> ClientUtils.waitTillContinuousClientsFinish(producerName, consumerNames.get(producerName.indexOf(producerName)), NAMESPACE, continuousClientsMessageCount));
        producerNames.forEach(producerName -> kubeClient().deleteJob(producerName));
        consumerNames.forEach(consumerName -> kubeClient().deleteJob(consumerName));
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        installClusterOperator(extensionContext, NAMESPACE);
    }

    @AfterEach
    void restore() {
        kubeClient().getClusterNodes().forEach(node -> setNodeSchedule(node.getMetadata().getName(), true));
    }

    private void drainNode(String nodeName) {
        LOGGER.info("Cluster node {} is going to drain", nodeName);
        setNodeSchedule(nodeName, false);
        cmdKubeClient().exec("adm", "drain", nodeName, "--delete-local-data", "--force", "--ignore-daemonsets");
    }

    private void setNodeSchedule(String node, boolean schedule) {
        LOGGER.info("Set {} schedule {}", node, schedule);
        cmdKubeClient().exec("adm", schedule ? "uncordon" : "cordon", node);
    }
}
