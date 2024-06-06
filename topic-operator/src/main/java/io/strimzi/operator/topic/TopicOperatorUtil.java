/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides utility methods for managing and interacting with KafkaTopic resources within a Topic Operator context. 
 * This class includes functionalities such as extracting topic names from KafkaTopic resources, managing reconciliation 
 * and operation timers, determining if a KafkaTopic is managed or paused, and handling common operations like reading 
 * file content and building HTTP authentication headers. It serves as a central point for operations that are frequently 
 * performed across different parts of the Topic Operator's codebase, ensuring consistency and reducing code duplication.
 */
public class TopicOperatorUtil {
    static final String MANAGED = "strimzi.io/managed";

    private TopicOperatorUtil() {
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
        List<String> result = reconcilableTopics.stream()
            .map(ReconcilableTopic::topicName)
            .collect(Collectors.toList());
        return result;
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
     * Whether the {@link KafkaTopic} is managed.
     *
     * @param kt Kafka topic.
     * @return True if the topic is managed.
     */
    public static boolean isManaged(KafkaTopic kt) {
        return kt.getMetadata() == null
            || kt.getMetadata().getAnnotations() == null
            || kt.getMetadata().getAnnotations().get(MANAGED) == null
            || !"false".equals(kt.getMetadata().getAnnotations().get(MANAGED));
    }

    /**
     * Whether the {@link KafkaTopic} is paused.
     *
     * @param kt Kafka topic.
     * @return True if the topic is paused.
     */
    public static boolean isPaused(KafkaTopic kt) {
        return Annotations.isReconciliationPausedWithAnnotation(kt);
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
}
