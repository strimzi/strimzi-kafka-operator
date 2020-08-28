/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.batch.JobBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;

import java.util.HashMap;
import java.util.Map;

// HTTP Bridge clients
public class KafkaBridgeClientsResource extends KafkaClientsResource {

    private final int port;
    private final int pollInterval;

    public KafkaBridgeClientsResource(String producerName, String consumerName, String bootstrapServer, String topicName,
                                      int messageCount, String additionalConfig, String consumerGroup, int port, int sendInterval, int pollInterval) {
        super(producerName, consumerName, bootstrapServer, topicName, messageCount, additionalConfig, consumerGroup, sendInterval);
        this.port = port;
        this.pollInterval = pollInterval;
    }

    public DoneableJob producerStrimziBridge() {
        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);
        producerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_BRIDGE_CLIENTS_LABEL_VALUE);

        return KubernetesResource.deployNewJob(new JobBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(producerLabels)
                .withName(producerName)
            .endMetadata()
            .withNewSpec()
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(producerLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withRestartPolicy("OnFailure")
                        .withContainers()
                            .addNewContainer()
                                .withName(producerName)
                                .withImage("strimzi/kafka-http-producer:latest")
                                .addNewEnv()
                                    .withName("HOSTNAME")
                                    .withValue(bootstrapServer)
                                .endEnv()
                                .addNewEnv()
                                    .withName("PORT")
                                    .withValue(Integer.toString(port))
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(topicName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("SEND_INTERVAL")
                                    .withValue(String.valueOf(delayMs))
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue(Integer.toString(messageCount))
                                .endEnv()
                            .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
    }

    public DoneableJob consumerStrimziBridge() {
        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", consumerName);
        consumerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_BRIDGE_CLIENTS_LABEL_VALUE);

        return KubernetesResource.deployNewJob(new JobBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(consumerLabels)
                .withName(consumerName)
            .endMetadata()
            .withNewSpec()
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(consumerLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withRestartPolicy("OnFailure")
                        .withContainers()
                            .addNewContainer()
                                .withName(consumerName)
                                .withImage("strimzi/kafka-http-consumer:latest")
                                .addNewEnv()
                                    .withName("HOSTNAME")
                                    .withValue(bootstrapServer)
                                .endEnv()
                                .addNewEnv()
                                    .withName("PORT")
                                    .withValue(Integer.toString(port))
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(topicName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("POLL_INTERVAL")
                                    .withValue(Integer.toString(pollInterval))
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue(Integer.toString(messageCount))
                                .endEnv()
                                .addNewEnv()
                                    .withName("GROUP_ID")
                                    .withValue(consumerGroup)
                                .endEnv()
                            .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
    }
}
