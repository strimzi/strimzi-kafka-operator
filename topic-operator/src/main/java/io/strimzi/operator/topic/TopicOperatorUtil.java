/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.model.Either;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.PartitionedByError;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.TopicOperatorException;
import org.apache.kafka.clients.admin.Admin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.operator.topic.KafkaHandler.BROKER_DEFAULT;
import static io.strimzi.operator.topic.KubernetesHandler.ANNO_STRIMZI_IO_MANAGED;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Provides utility methods for managing and interacting 
 * with KafkaTopic resources within a Topic Operator context.
 */
public class TopicOperatorUtil {
    private TopicOperatorUtil() { }

    /**
     * Create a new Kubernetes client instance.
     *
     * @return Kubernetes client.
     */
    public static KubernetesClient createKubernetesClient() {
        return new OperatorKubernetesClientBuilder(
            "strimzi-topic-operator-" + UUID.randomUUID(),
            TopicOperatorMain.class.getPackage().getImplementationVersion())
            .build();
    }

    /**
     * Create a new Kafka admin client instance.
     *
     * @param config Topic Operator configuration.
     * @return Kafka admin client.
     */
    public static Admin createKafkaAdminClient(TopicOperatorConfig config) {
        return Admin.create(config.adminClientConfig());
    }

    /**
     * Create a new Cruise Control client instance.
     *
     * @param config Topic Operator configuration.
     * @return Cruise Control client.
     */
    public static CruiseControlClient createCruiseControlClient(TopicOperatorConfig config) {
        return CruiseControlClient.create(
            config.cruiseControlHostname(),
            config.cruiseControlPort(),
            config.cruiseControlRackEnabled(),
            config.cruiseControlSslEnabled(),
            config.cruiseControlSslEnabled() ? TopicOperatorUtil.getFileContent(config.cruiseControlCrtFilePath()) : null,
            config.cruiseControlAuthEnabled(),
            config.cruiseControlAuthEnabled() ? new String(TopicOperatorUtil.getFileContent(config.cruiseControlApiUserPath()), UTF_8) : null,
            config.cruiseControlAuthEnabled() ? new String(TopicOperatorUtil.getFileContent(config.cruiseControlApiPassPath()), UTF_8) : null
        );
    }

    private static byte[] getFileContent(String filePath) {
        try {
            return Files.readAllBytes(Path.of(filePath));
        } catch (IOException ioe) {
            throw new UncheckedIOException(format("File not found: %s", filePath), ioe);
        }
    }

    /**
     * Get the topic name from a {@link KafkaTopic} resource.
     *
     * @param kafkaTopic Topic resource.
     * @return Topic name.
     */
    public static String topicName(KafkaTopic kafkaTopic) {
        String name = null;
        if (kafkaTopic.getSpec() != null) {
            name = kafkaTopic.getSpec().getTopicName();
        }
        if (name == null) {
            name = kafkaTopic.getMetadata().getName();
        }
        return name;
    }

    /**
     * Get topic names from reconcilable topics.
     *
     * @param reconcilableTopics Reconcilable topics.
     * @return Topic names
     */
    public static List<String> topicNames(List<ReconcilableTopic> reconcilableTopics) {
        return reconcilableTopics.stream()
            .map(ReconcilableTopic::topicName)
            .collect(Collectors.toList());
    }

    /**
     * Start the reconciliation timer.
     *
     * @param metricsHolder Metrics holder.
     * @return Timer sample.
     */
    public static Timer.Sample startReconciliationTimer(TopicOperatorMetricsHolder metricsHolder) {
        return Timer.start(metricsHolder.metricsProvider().meterRegistry());
    }

    /**
     * Stop the reconciliation timer.
     *
     * @param metricsHolder Metrics holder.
     * @param timerSample Timer sample.
     * @param namespace Namespace.
     */
    public static void stopReconciliationTimer(TopicOperatorMetricsHolder metricsHolder,
                                               Timer.Sample timerSample,
                                               String namespace) {
        timerSample.stop(metricsHolder.reconciliationsTimer(namespace));
    }

    /**
     * Start the external request timer.
     *
     * @param metricsHolder Metrics holder.
     * @param enabled Whether additional metricsHolder are enabled.
     * @return Timer sample.
     */
    public static Timer.Sample startExternalRequestTimer(TopicOperatorMetricsHolder metricsHolder,
                                                         boolean enabled) {
        return enabled ? Timer.start(metricsHolder.metricsProvider().meterRegistry()) : null;
    }

    /**
     * Stop the external request timer.
     *
     * @param timerSample Timer sample.
     * @param requestTimer Request timer.
     * @param enabled Whether additional metrics are enabled.
     * @param namespace Namespace.
     */
    public static void stopExternalRequestTimer(Timer.Sample timerSample,
                                                Function<String, Timer> requestTimer,
                                                boolean enabled,
                                                String namespace) {
        if (enabled) {
            timerSample.stop(requestTimer.apply(namespace));
        }
    }

    /**
     * Whether {@link KafkaTopic} is managed.
     *
     * @param kafkaTopic Kafka topic.
     * @return True if the topic is managed.
     */
    public static boolean isManaged(KafkaTopic kafkaTopic) {
        return kafkaTopic.getMetadata() == null
            || kafkaTopic.getMetadata().getAnnotations() == null
            || kafkaTopic.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_MANAGED) == null
            || !"false".equals(kafkaTopic.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_MANAGED));
    }

    /**
     * Whether {@link KafkaTopic} is paused.
     *
     * @param kafkaTopic Kafka topic.
     * @return True if the topic is paused.
     */
    public static boolean isPaused(KafkaTopic kafkaTopic) {
        return Annotations.isReconciliationPausedWithAnnotation(kafkaTopic);
    }

    /**
     * Whether the {@link KafkaTopic} status has config.
     *
     * @param kafkaTopic Kafka topic.
     * @return True if there is config.
     */
    public static boolean hasConfig(KafkaTopic kafkaTopic) {
        return kafkaTopic.getSpec() != null && kafkaTopic.getSpec().getConfig() != null;
    }

    /**
     * Whether the {@link KafkaTopic} status has replicas change status.
     *
     * @param kafkaTopic Kafka topic.
     * @return True if there is replicas change status.
     */
    public static boolean hasReplicasChangeStatus(KafkaTopic kafkaTopic) {
        return kafkaTopic.getStatus() != null && kafkaTopic.getStatus().getReplicasChange() != null;
    }

    /**
     * Get KafkaTopic resource version.
     *
     * @param kafkaTopic Kafka topic.
     * @return Resource version, or the string "null" if there isn't a resource version.
     */
    public static String resourceVersion(KafkaTopic kafkaTopic) {
        return kafkaTopic == null || kafkaTopic.getMetadata() == null ? "null" : kafkaTopic.getMetadata().getResourceVersion();
    }

    /**
     * Get number of partitions from {@link KafkaTopic} spec.
     *
     * @param kafkaTopic KafkaTopic resource.
     * @return Partitions value.
     */
    public static int partitions(KafkaTopic kafkaTopic) {
        return kafkaTopic.getSpec().getPartitions() != null ? kafkaTopic.getSpec().getPartitions() : BROKER_DEFAULT;
    }

    /**
     * Get number of replicas from {@link KafkaTopic} spec.
     *
     * @param kafkaTopic KafkaTopic resource.
     * @return Replicas value.
     */
    public static short replicas(KafkaTopic kafkaTopic) {
        return kafkaTopic.getSpec().getReplicas() != null ? kafkaTopic.getSpec().getReplicas().shortValue() : BROKER_DEFAULT;
    }

    /**
     * Partition the input stream {@link Pair}s into success and error lists.
     *
     * @param stream Input stream of pairs.
     * @return Pairs partitioned by error.
     * @param <K> Type of pair key.
     * @param <V> Type of pair value.
     */
    public static <K, V> PartitionedByError<K, V> partitionedByError(Stream<Pair<K, Either<TopicOperatorException, V>>> stream) {
        var collect = stream.collect(Collectors.partitioningBy(x -> x.getValue().isRight()));
        return new PartitionedByError<>(collect.get(true), collect.get(false));
    }

    /**
     * Get Kafka configuration value as string.
     *
     * @param value Configuration name.
     * @return Value as string.
     */
    public static String configValueAsString(Object value) {
        String valueStr;
        if (value instanceof String || value instanceof Boolean) {
            valueStr = value.toString();
        } else if (value instanceof Number) {
            valueStr = value.toString();
        } else if (value instanceof List) {
            valueStr = ((List<?>) value).stream()
                .map(TopicOperatorUtil::configValueAsString)
                .collect(Collectors.joining(","));
        } else {
            throw new RuntimeException("Cannot convert " + value);
        }
        return valueStr;
    }
}
