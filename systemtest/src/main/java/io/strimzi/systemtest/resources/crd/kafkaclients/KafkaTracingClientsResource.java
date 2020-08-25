/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd.kafkaclients;

import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;

import java.util.HashMap;
import java.util.Map;

public class KafkaTracingClientsResource extends KafkaClientsResource {

    private static final String JAEGER_AGENT_HOST =  "my-jaeger-agent";
    private static final String JAEGER_SAMPLER_TYPE =  "const";
    private static final String JAEGER_SAMPLER_PARAM =  "1";

    final String jaegerServiceProducerName;
    final String jaegerServiceConsumerName;
    final String jaegerServiceStreamsName;

    public KafkaTracingClientsResource(String producerName, String consumerName, String bootstrapServer, String topicName,
                                       int messageCount, String additionalConfig, String consumerGroup, String jaegerServiceProducerName,
                                       String jaegerServiceConsumerName, String jaegerServiceStreamsName) {

        super(producerName, consumerName, bootstrapServer, topicName, messageCount, additionalConfig, consumerGroup);
        this.jaegerServiceProducerName =  jaegerServiceProducerName;
        this.jaegerServiceConsumerName =  jaegerServiceConsumerName;
        this.jaegerServiceStreamsName = jaegerServiceStreamsName;
    }

    // from existing client create new client with different bootstrapServer + topicName (immutability)
    public KafkaTracingClientsResource(KafkaTracingClientsResource kafkaTracingClientsResource, String bootstrapServer,
                                       String topicName) {

        super(kafkaTracingClientsResource.producerName, kafkaTracingClientsResource.consumerName, bootstrapServer, topicName,
            kafkaTracingClientsResource.messageCount, kafkaTracingClientsResource.additionalConfig, kafkaTracingClientsResource.consumerGroup);
        this.jaegerServiceProducerName =  kafkaTracingClientsResource.jaegerServiceProducerName;
        this.jaegerServiceConsumerName =  kafkaTracingClientsResource.jaegerServiceConsumerName;
        this.jaegerServiceStreamsName = kafkaTracingClientsResource.jaegerServiceStreamsName;
    }

    // from existing client create new client with different bootstrapServer (immutability)
    public KafkaTracingClientsResource(KafkaTracingClientsResource kafkaTracingClientsResource, String bootstrapServer) {

        super(kafkaTracingClientsResource.producerName, kafkaTracingClientsResource.consumerName, bootstrapServer,
            kafkaTracingClientsResource.topicName, kafkaTracingClientsResource.messageCount,
            kafkaTracingClientsResource.additionalConfig, kafkaTracingClientsResource.consumerGroup);
        this.jaegerServiceProducerName =  kafkaTracingClientsResource.jaegerServiceProducerName;
        this.jaegerServiceConsumerName =  kafkaTracingClientsResource.jaegerServiceConsumerName;
        this.jaegerServiceStreamsName = kafkaTracingClientsResource.jaegerServiceStreamsName;
    }

    public DoneableDeployment consumerWithTracing() {
        String consumerName = "hello-world-consumer";

        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", consumerName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
                    .withNewMetadata()
                        .withNamespace(ResourceManager.kubeClient().getNamespace())
                        .withLabels(consumerLabels)
                        .withName(consumerName)
                    .endMetadata()
                    .withNewSpec()
                        .withNewSelector()
                            .withMatchLabels(consumerLabels)
                        .endSelector()
                        .withReplicas(1)
                        .withNewTemplate()
                            .withNewMetadata()
                                .withLabels(consumerLabels)
                            .endMetadata()
                            .withNewSpec()
                                .withContainers()
                                .addNewContainer()
                                    .withName(consumerName)
                                    .withImage("strimzi/" + consumerName + ":latest")
                                    .addNewEnv()
                                        .withName("BOOTSTRAP_SERVERS")
                                        .withValue(bootstrapServer)
                                      .endEnv()
                                    .addNewEnv()
                                        .withName("TOPIC")
                                        .withValue(topicName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("GROUP_ID")
                                        .withValue("my-" + consumerName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("DELAY_MS")
                                        .withValue("1000")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("LOG_LEVEL")
                                        .withValue("INFO")
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("MESSAGE_COUNT")
                                        .withValue(String.valueOf(messageCount))
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SERVICE_NAME")
                                        .withValue(jaegerServiceConsumerName)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_AGENT_HOST")
                                        .withValue(JAEGER_AGENT_HOST)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_TYPE")
                                        .withValue(JAEGER_SAMPLER_TYPE)
                                    .endEnv()
                                    .addNewEnv()
                                        .withName("JAEGER_SAMPLER_PARAM")
                                        .withValue(JAEGER_SAMPLER_PARAM)
                                    .endEnv()
                                .endContainer()
                            .endSpec()
                        .endTemplate()
                    .endSpec()
                    .build());
    }

    public DoneableDeployment producerWithTracing() {
        String producerName = "hello-world-producer";

        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", producerName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(producerLabels)
                .withName(producerName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .withMatchLabels(producerLabels)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(producerLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withContainers()
                        .addNewContainer()
                            .withName(producerName)
                            .withImage("strimzi/" + producerName + ":latest")
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(bootstrapServer)
                              .endEnv()
                            .addNewEnv()
                                .withName("TOPIC")
                                .withValue("my-topic")
                            .endEnv()
                            .addNewEnv()
                                .withName("DELAY_MS")
                                .withValue("1000")
                            .endEnv()
                            .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("INFO")
                            .endEnv()
                            .addNewEnv()
                                .withName("MESSAGE_COUNT")
                                .withValue(String.valueOf(messageCount))
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(jaegerServiceProducerName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_HOST)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
    }

    public DoneableDeployment kafkaStreamsWithTracing() {
        String kafkaStreamsName = "hello-world-streams";

        Map<String, String> kafkaStreamLabels = new HashMap<>();
        kafkaStreamLabels.put("app", kafkaStreamsName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(kafkaStreamLabels)
                .withName(kafkaStreamsName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .withMatchLabels(kafkaStreamLabels)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(kafkaStreamLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withContainers()
                        .addNewContainer()
                            .withName(kafkaStreamsName)
                            .withImage("strimzi/" + kafkaStreamsName + ":latest")
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(bootstrapServer)
                              .endEnv()
                            .addNewEnv()
                                .withName("APPLICATION_ID")
                                .withValue(kafkaStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("SOURCE_TOPIC")
                                .withValue("my-topic")
                            .endEnv()
                            .addNewEnv()
                                .withName("TARGET_TOPIC")
                                .withValue("cipot-ym")
                            .endEnv()
                              .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("INFO")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(jaegerServiceStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_HOST)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
    }
}
