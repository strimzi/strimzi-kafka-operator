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

public class KafkaBasicClientResource extends KafkaClientsResource {

    public KafkaBasicClientResource(String producerName, String consumerName, String bootstrapServer, String topicName,
                                    int messageCount, String additionalConfig, String consumerGroup, long delayMs) {
        super(producerName, consumerName, bootstrapServer, topicName, messageCount, additionalConfig, consumerGroup, delayMs);
    }

    public KafkaBasicClientResource(KafkaClientsResource kafkaClientsResource) {
        super(kafkaClientsResource);
    }

    public KafkaBasicClientResource(KafkaClientsResource kafkaClientsResource, String topicName, String consumerGroup) {
        super(kafkaClientsResource, topicName, consumerGroup);
    }

    public DoneableJob producerStrimzi() {
        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);
        producerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE);

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
                                .withImagePullPolicy("IfNotPresent")
                                .withImage("strimzi/hello-world-producer:latest")
                                .addNewEnv()
                                    .withName("BOOTSTRAP_SERVERS")
                                    .withValue(bootstrapServer)
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(topicName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("DELAY_MS")
                                    .withValue(String.valueOf(delayMs))
                                .endEnv()
                                .addNewEnv()
                                    .withName("LOG_LEVEL")
                                    .withValue("DEBUG")
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue(String.valueOf(messageCount))
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE")
                                    .withValue("Hello-world")
                                .endEnv()
                                .addNewEnv()
                                    .withName("PRODUCER_ACKS")
                                    .withValue("all")
                                .endEnv()
                                .addNewEnv()
                                    .withName("ADDITIONAL_CONFIG")
                                    .withValue(additionalConfig)
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build());
    }

    public DoneableJob consumerStrimzi() {
        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", consumerName);
        consumerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_CLIENTS_LABEL_VALUE);

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
                                .withImagePullPolicy("IfNotPresent")
                                .withImage("strimzi/hello-world-consumer:latest")
                                .addNewEnv()
                                    .withName("BOOTSTRAP_SERVERS")
                                    .withValue(bootstrapServer)
                                .endEnv()
                                .addNewEnv()
                                    .withName("TOPIC")
                                    .withValue(topicName)
                                .endEnv()
                                .addNewEnv()
                                    .withName("DELAY_MS")
                                    .withValue(String.valueOf(delayMs))
                                .endEnv()
                                .addNewEnv()
                                    .withName("LOG_LEVEL")
                                    .withValue("DEBUG")
                                .endEnv()
                                .addNewEnv()
                                    .withName("MESSAGE_COUNT")
                                    .withValue(String.valueOf(messageCount))
                                .endEnv()
                               .addNewEnv()
                                    .withName("GROUP_ID")
                                    .withValue(consumerGroup)
                                .endEnv()
                                .addNewEnv()
                                    .withName("ADDITIONAL_CONFIG")
                                    .withValue(additionalConfig)
                                .endEnv()
                                .addNewEnv()
                                    .withName("BLOCKING_PRODUCER")
                                    .withValue("true")
                                .endEnv()
                            .endContainer()
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build());
    }
}
