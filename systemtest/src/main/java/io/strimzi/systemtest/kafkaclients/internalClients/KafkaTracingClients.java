/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.internalClients;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.tracing.TracingConstants;
import io.sundr.builder.annotations.Buildable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Buildable(editableEnabled = false)
public class KafkaTracingClients extends KafkaClients {
    private String jaegerServiceProducerName;
    private String jaegerServiceConsumerName;
    private String jaegerServiceStreamsName;
    private String streamsTopicTargetName;
    private String tracingServiceNameEnvVar;
    private boolean openTelemetry = false;
    private String tracingType;

    public String getJaegerServiceConsumerName() {
        return jaegerServiceConsumerName;
    }

    public void setJaegerServiceConsumerName(String jaegerServiceConsumerName) {
        this.jaegerServiceConsumerName = jaegerServiceConsumerName;
    }

    public String getJaegerServiceProducerName() {
        return jaegerServiceProducerName;
    }

    public void setJaegerServiceProducerName(String jaegerServiceProducerName) {
        this.jaegerServiceProducerName = jaegerServiceProducerName;
    }

    public String getJaegerServiceStreamsName() {
        return jaegerServiceStreamsName;
    }

    public void setJaegerServiceStreamsName(String jaegerServiceStreamsName) {
        this.jaegerServiceStreamsName = jaegerServiceStreamsName;
    }

    public String getStreamsTopicTargetName() {
        return streamsTopicTargetName;
    }

    public void setStreamsTopicTargetName(String streamsTopicTargetName) {
        this.streamsTopicTargetName = streamsTopicTargetName;
    }

    public String getTracingServiceNameEnvVar() {
        return tracingServiceNameEnvVar;
    }

    public void setTracingServiceNameEnvVar(String tracingServiceNameEnvVar) {
        this.tracingServiceNameEnvVar = tracingServiceNameEnvVar;
    }

    public void setOpenTelemetry(boolean openTelemetry) {
        this.openTelemetry = openTelemetry;
    }

    public boolean getOpenTelemetry() {
        return openTelemetry;
    }

    public void setTracingType(String tracingType) {
        // if `withOpenTelemetry` or `withOpenTracing` is used, this is the only way how to set it also as the tracingType
        // to remove need of extra check in each client's method
        if (this.openTelemetry) {
            this.tracingType = TracingConstants.OPEN_TELEMETRY;
        } else {
            this.tracingType = tracingType;
        }
    }

    public String getTracingType() {
        return tracingType;
    }

    public Job consumerWithTracing() {
        return defaultConsumerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName(this.tracingServiceNameEnvVar)
                                .withValue(this.jaegerServiceConsumerName)
                            .endEnv()
                            .addNewEnv()
                                .withName("TRACING_TYPE")
                                .withValue(this.tracingType)
                            .endEnv()
                            .addNewEnv()
                                .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                                .withValue(TracingConstants.JAEGER_COLLECTOR_OTLP_URL)
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    public Job producerWithTracing() {
        return defaultProducerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName(this.tracingServiceNameEnvVar)
                                .withValue(this.jaegerServiceProducerName)
                            .endEnv()
                            .addNewEnv()
                                .withName("TRACING_TYPE")
                                .withValue(this.tracingType)
                            .endEnv()
                            .addNewEnv()
                                .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                                .withValue(TracingConstants.JAEGER_COLLECTOR_OTLP_URL)
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    public Job kafkaStreamsWithTracing() {
        String kafkaStreamsName = "hello-world-streams";

        Map<String, String> kafkaStreamLabels = new HashMap<>();
        kafkaStreamLabels.put("app", kafkaStreamsName);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        return new JobBuilder()
            .withNewMetadata()
                .withNamespace(getNamespaceName())
                .withLabels(kafkaStreamLabels)
                .withName(kafkaStreamsName)
            .endMetadata()
            .withNewSpec()
                .withBackoffLimit(0)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(kafkaStreamLabels)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withRestartPolicy("Never")
                        .withContainers()
                        .addNewContainer()
                            .withName(kafkaStreamsName)
                            .withImage(Environment.TEST_CLIENTS_IMAGE)
                            .addNewEnv()
                                .withName("BOOTSTRAP_SERVERS")
                                .withValue(this.getBootstrapAddress())
                              .endEnv()
                            .addNewEnv()
                                .withName("APPLICATION_ID")
                                .withValue(kafkaStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("SOURCE_TOPIC")
                                .withValue(this.getTopicName())
                            .endEnv()
                            .addNewEnv()
                                .withName("TARGET_TOPIC")
                                .withValue(this.streamsTopicTargetName)
                            .endEnv()
                              .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("DEBUG")
                            .endEnv()
                            .addNewEnv()
                                .withName(this.tracingServiceNameEnvVar)
                                .withValue(this.jaegerServiceStreamsName)
                            .endEnv()
                            .addNewEnv()
                                .withName("OTEL_EXPORTER_OTLP_ENDPOINT")
                                .withValue(TracingConstants.JAEGER_COLLECTOR_OTLP_URL)
                            .endEnv()
                            .addNewEnv()
                                .withName("TRACING_TYPE")
                                .withValue(this.tracingType)
                            .endEnv()
                            .addNewEnv()
                                .withName("CLIENT_TYPE")
                                .withValue("KafkaStreams")
                            .endEnv()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }
}
