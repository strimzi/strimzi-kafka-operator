/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.sundr.builder.annotations.Buildable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Buildable(editableEnabled = false)
public class BridgeClients extends KafkaClients {
    private String componentName;
    private int pollInterval;
    private int port;

    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    public int getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(int pollInterval) {
        this.pollInterval = pollInterval;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    private void createNetworkPoliciesIfNeeded(String clientName, Map<String, String> clientLabels) {
        // We need to create network policies if default policy is to deny traffic
        if (Environment.DEFAULT_TO_DENY_NETWORK_POLICIES) {
            LabelSelector producerLabelSelector = new LabelSelectorBuilder()
                .addToMatchLabels(clientLabels)
                .build();

            NetworkPolicyResource.allowNetworkPolicySettingsForBridgeClients(this.getNamespaceName(), clientName, producerLabelSelector, this.getComponentName());
        }
    }

    public JobBuilder defaultProducerStrimziBridge() {
        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", this.getProducerName());
        producerLabels.put(TestConstants.KAFKA_CLIENTS_LABEL_KEY, TestConstants.KAFKA_BRIDGE_CLIENTS_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        createNetworkPoliciesIfNeeded(this.getProducerName(), producerLabels);

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(this.getNamespaceName())
                .withLabels(producerLabels)
                .withName(this.getProducerName())
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(producerLabels)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withRestartPolicy("Never")
                        .addNewContainer()
                            .withName(this.getProducerName())
                            .withImagePullPolicy(TestConstants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                            .withImage(Environment.TEST_CLIENTS_IMAGE)
                            .addNewEnv()
                                .withName("HOSTNAME")
                                .withValue(this.getBootstrapAddress())
                            .endEnv()
                            .addNewEnv()
                                .withName("PORT")
                                .withValue(Integer.toString(port))
                            .endEnv()
                            .addNewEnv()
                                .withName("TOPIC")
                                .withValue(this.getTopicName())
                            .endEnv()
                            .addNewEnv()
                                .withName("DELAY_MS")
                                .withValue(String.valueOf(this.getDelayMs()))
                            .endEnv()
                            .addNewEnv()
                                .withName("MESSAGE_COUNT")
                                .withValue(Integer.toString(this.getMessageCount()))
                            .endEnv()
                            .addNewEnv()
                                .withName("CLIENT_TYPE")
                                .withValue("HttpProducer")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }


    public Job producerStrimziBridge() {
        return this.defaultProducerStrimziBridge().build();
    }

    public JobBuilder defaultConsumerStrimziBridge() {
        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", this.getConsumerName());
        consumerLabels.put(TestConstants.KAFKA_CLIENTS_LABEL_KEY, TestConstants.KAFKA_BRIDGE_CLIENTS_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        createNetworkPoliciesIfNeeded(this.getConsumerName(), consumerLabels);

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(this.getNamespaceName())
                .withLabels(consumerLabels)
                .withName(this.getConsumerName())
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(consumerLabels)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withRestartPolicy("Never")
                        .addNewContainer()
                            .withName(this.getConsumerName())
                            .withImagePullPolicy(TestConstants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                            .withImage(Environment.TEST_CLIENTS_IMAGE)
                            .addNewEnv()
                                .withName("HOSTNAME")
                                .withValue(this.getBootstrapAddress())
                            .endEnv()
                            .addNewEnv()
                                .withName("PORT")
                                .withValue(Integer.toString(port))
                            .endEnv()
                            .addNewEnv()
                                .withName("TOPIC")
                                .withValue(this.getTopicName())
                            .endEnv()
                            .addNewEnv()
                                .withName("POLL_INTERVAL")
                                .withValue(Integer.toString(pollInterval))
                            .endEnv()
                            .addNewEnv()
                                .withName("MESSAGE_COUNT")
                                .withValue(Integer.toString(this.getMessageCount()))
                            .endEnv()
                            .addNewEnv()
                                .withName("GROUP_ID")
                                .withValue(this.getConsumerGroup())
                            .endEnv()
                            .addNewEnv()
                                .withName("CLIENT_TYPE")
                                .withValue("HttpConsumer")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }

    public Job consumerStrimziBridge() {
        return this.defaultConsumerStrimziBridge().build();
    }
}
