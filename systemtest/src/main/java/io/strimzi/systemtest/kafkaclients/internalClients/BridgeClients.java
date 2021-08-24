/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.sundr.builder.annotations.Buildable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_AGENT_HOST;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_COLLECTOR_URL;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_SAMPLER_PARAM;
import static io.strimzi.systemtest.tracing.TracingConstants.JAEGER_SAMPLER_TYPE;

@Buildable(editableEnabled = false)
public class BridgeClients extends KafkaClients {
    private int port;
    private int pollInterval;

    private String tracingServiceNameEnvVar;

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

    public String getTracingServiceNameEnvVar() {
        return tracingServiceNameEnvVar;
    }

    public void setTracingServiceNameEnvVar(String tracingServiceNameEnvVar) {
        this.tracingServiceNameEnvVar = tracingServiceNameEnvVar;
    }

    private String serviceNameEnvVar() {
        // use tracing service name env var if set, else use "dummy"
        return tracingServiceNameEnvVar != null ? tracingServiceNameEnvVar : "TEST_SERVICE_NAME";
    }

    public Job producerStrimziBridge() {
        Map<String, String> producerLabels = new HashMap<>();
        producerLabels.put("app", this.getProducerName());
        producerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_BRIDGE_CLIENTS_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

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
                        .withRestartPolicy("OnFailure")
                        .addNewContainer()
                            .withName(this.getProducerName())
                            .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                            .withImage(Environment.TEST_HTTP_PRODUCER_IMAGE)
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
                                .withName(serviceNameEnvVar())
                                .withValue(getProducerName())
                            .endEnv()
                            // this will only get used if tracing is enabled -- see serviceNameEnvVar()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_HOST)
                            .endEnv()
                            .addNewEnv()
                                .withName("OTEL_EXPORTER_JAEGER_ENDPOINT")
                                .withValue(JAEGER_COLLECTOR_URL)
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
            .build();
    }

    public Job consumerStrimziBridge() {
        Map<String, String> consumerLabels = new HashMap<>();
        consumerLabels.put("app", this.getConsumerName());
        consumerLabels.put(Constants.KAFKA_CLIENTS_LABEL_KEY, Constants.KAFKA_BRIDGE_CLIENTS_LABEL_VALUE);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

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
                        .withRestartPolicy("OnFailure")
                        .addNewContainer()
                            .withName(this.getConsumerName())
                            .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                            .withImage(Environment.TEST_HTTP_CONSUMER_IMAGE)
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
                                .withName(serviceNameEnvVar())
                                .withValue(getConsumerName())
                            .endEnv()
                            // this will only get used if tracing is enabled -- see serviceNameEnvVar()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_HOST)
                            .endEnv()
                            .addNewEnv()
                                .withName("OTEL_EXPORTER_JAEGER_ENDPOINT")
                                .withValue(JAEGER_COLLECTOR_URL)
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
            .build();
    }
}
