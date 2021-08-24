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
import io.strimzi.systemtest.resources.ResourceManager;
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
public class KafkaTracingClients  extends KafkaClients {

    private String tracingServiceNameEnvVar;
    private String jaegerServiceProducerName;
    private String jaegerServiceConsumerName;
    private String jaegerServiceStreamsName;
    private String jaegerServerAgentName;
    private String streamsTopicTargetName;

    public String getTracingServiceNameEnvVar() {
        return tracingServiceNameEnvVar;
    }

    public void setTracingServiceNameEnvVar(String tracingServiceNameEnvVar) {
        this.tracingServiceNameEnvVar = tracingServiceNameEnvVar;
    }

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

    public String getJaegerServerAgentName() {
        return jaegerServerAgentName;
    }

    public void setJaegerServerAgentName(String jaegerServerAgentName) {
        this.jaegerServerAgentName = jaegerServerAgentName;
    }

    public String getStreamsTopicTargetName() {
        return streamsTopicTargetName;
    }

    public void setStreamsTopicTargetName(String streamsTopicTargetName) {
        this.streamsTopicTargetName = streamsTopicTargetName;
    }

    public Job consumerWithTracing() {
        return defaultConsumerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName(tracingServiceNameEnvVar)
                                .withValue(jaegerServiceConsumerName)
                            .endEnv()
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

    public Job producerWithTracing() {
        return defaultProducerStrimzi()
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .addNewEnv()
                                .withName(tracingServiceNameEnvVar)
                                .withValue(jaegerServiceProducerName)
                            .endEnv()
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
                .withNamespace(ResourceManager.kubeClient().getNamespace())
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
                            .withImagePullPolicy(Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY)
                            .withImage(Environment.TEST_STREAMS_IMAGE)
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
                                .withValue(streamsTopicTargetName)
                            .endEnv()
                              .addNewEnv()
                                .withName("LOG_LEVEL")
                                .withValue("DEBUG")
                            .endEnv()
                            .addNewEnv()
                                .withName(tracingServiceNameEnvVar)
                                .withValue(jaegerServiceStreamsName)
                            .endEnv()
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
