/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.OperatorKubernetesClientBuilder;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.model.Either;
import io.strimzi.operator.topic.model.Pair;
import io.strimzi.operator.topic.model.PartitionedByError;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.TopicOperatorException;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.operator.topic.KafkaHandler.BROKER_DEFAULT;
import static io.strimzi.operator.topic.KubernetesHandler.ANNO_STRIMZI_IO_MANAGED;

/**
 * Provides utility methods for managing and interacting 
 * with KafkaTopic resources within a Topic Operator context.
 */
public class TopicOperatorUtil {
    private TopicOperatorUtil() { }

    /**
     * Create e new instance of Kubernetes client.
     * 
     * @return Kubernetes client.
     */
    public static KubernetesClient createKubeClient() {
        return new OperatorKubernetesClientBuilder(
            "strimzi-topic-operator",
            TopicOperatorMain.class.getPackage().getImplementationVersion())
            .build();
    }

    /**
     * Get the topic name from a {@link KafkaTopic} resource.
     *
     * @param kafkaTopic Topic resource.
     * @return Topic name.
     */
    public static String topicName(KafkaTopic kafkaTopic) {
        String tn = null;
        if (kafkaTopic.getSpec() != null) {
            tn = kafkaTopic.getSpec().getTopicName();
        }
        if (tn == null) {
            tn = kafkaTopic.getMetadata().getName();
        }
        return tn;
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
     * @param metrics Metrics holder.
     * @return Timer sample.
     */
    public static Timer.Sample startReconciliationTimer(TopicOperatorMetricsHolder metrics) {
        return Timer.start(metrics.metricsProvider().meterRegistry());
    }

    /**
     * Stop the reconciliation timer.
     *
     * @param metrics Metrics holder.
     * @param timerSample Timer sample.
     * @param namespace Namespace.
     */
    public static void stopReconciliationTimer(TopicOperatorMetricsHolder metrics,
                                               Timer.Sample timerSample,
                                               String namespace) {
        timerSample.stop(metrics.reconciliationsTimer(namespace));
    }

    /**
     * Start the external request timer.
     *
     * @param metrics Metrics holder.
     * @param enabled Whether additional metrics are enabled.
     * @return Timer sample.
     */
    public static Timer.Sample startExternalRequestTimer(TopicOperatorMetricsHolder metrics,
                                                         boolean enabled) {
        return enabled ? Timer.start(metrics.metricsProvider().meterRegistry()) : null;
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
     * @param kt Kafka topic.
     * @return True if the topic is managed.
     */
    public static boolean isManaged(KafkaTopic kt) {
        return kt.getMetadata() == null
            || kt.getMetadata().getAnnotations() == null
            || kt.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_MANAGED) == null
            || !"false".equals(kt.getMetadata().getAnnotations().get(ANNO_STRIMZI_IO_MANAGED));
    }

    /**
     * Whether {@link KafkaTopic} is paused.
     *
     * @param kt Kafka topic.
     * @return True if the topic is paused.
     */
    public static boolean isPaused(KafkaTopic kt) {
        return Annotations.isReconciliationPausedWithAnnotation(kt);
    }

    /**
     * Whether the {@link KafkaTopic} status has config.
     *
     * @param kt Kafka topic.
     * @return True if there is config.
     */
    public static boolean hasConfig(KafkaTopic kt) {
        return kt.getSpec() != null && kt.getSpec().getConfig() != null;
    }

    /**
     * Whether the {@link KafkaTopic} status has replicas change.
     *
     * @param status Topic status.
     * @return True if there is replicas change status.
     */
    public static boolean hasReplicasChange(KafkaTopicStatus status) {
        return status != null && status.getReplicasChange() != null;
    }

    /**
     * Get KafkaTopic resource version.
     *
     * @param kt Kafka topic.
     * @return Resource version.
     */
    public static String resourceVersion(KafkaTopic kt) {
        return kt == null || kt.getMetadata() == null ? "null" : kt.getMetadata().getResourceVersion();
    }

    /**
     * Get number of partitions from {@link KafkaTopic} spec.
     *
     * @param kt KafkaTopic resource.
     * @return Partitions value.
     */
    public static int partitions(KafkaTopic kt) {
        return kt.getSpec().getPartitions() != null ? kt.getSpec().getPartitions() : BROKER_DEFAULT;
    }

    /**
     * Get number of replicas from {@link KafkaTopic} spec.
     *
     * @param kt KafkaTopic resource.
     * @return Replicas value.
     */
    public static short replicas(KafkaTopic kt) {
        return kt.getSpec().getReplicas() != null ? kt.getSpec().getReplicas().shortValue() : BROKER_DEFAULT;
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
