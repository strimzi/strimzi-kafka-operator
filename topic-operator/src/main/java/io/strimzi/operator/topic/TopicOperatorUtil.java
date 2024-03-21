/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.String.join;

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
     * @param reconcilableTopic Reconcilable topic.
     * @param metrics Metrics holder.
     */
    public static void startReconciliationTimer(ReconcilableTopic reconcilableTopic,
                                                TopicOperatorMetricsHolder metrics) {
        if (reconcilableTopic.reconciliationTimerSample() == null) {
            reconcilableTopic.reconciliationTimerSample(Timer.start(metrics.metricsProvider().meterRegistry()));
        }
    }

    /**
     * Stop the reconciliation timer.
     * 
     * @param reconcilableTopic Reconcilable topic.
     * @param metrics Metrics holder.
     * @param namespace Namespace.
     */
    public static void stopReconciliationTimer(ReconcilableTopic reconcilableTopic,
                                               TopicOperatorMetricsHolder metrics,
                                               String namespace) {
        if (reconcilableTopic.reconciliationTimerSample() != null) {
            reconcilableTopic.reconciliationTimerSample().stop(metrics.reconciliationsTimer(namespace));
        }
    }

    /**
     * Start the operation timer.
     * 
     * @param enableAdditionalMetrics Whether to enable additional metrics.
     * @param metrics Metrics holder.
     * @return The timer sample.
     */
    public static Timer.Sample startOperationTimer(boolean enableAdditionalMetrics,
                                                   TopicOperatorMetricsHolder metrics) {
        if (enableAdditionalMetrics) {
            return Timer.start(metrics.metricsProvider().meterRegistry());
        } else {
            return null;
        }
    }

    /**
     * Stop the operation timer.
     *
     * @param timerSample The timer sample.
     * @param opTimer Operation timer.
     * @param enableAdditionalMetrics Whether to enable additional metrics.
     * @param namespace Namespace.
     */
    public static void stopOperationTimer(Timer.Sample timerSample, 
                                          Function<String, Timer> opTimer,
                                          boolean enableAdditionalMetrics,
                                          String namespace) {
        if (timerSample != null && enableAdditionalMetrics) {
            timerSample.stop(opTimer.apply(namespace));
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
     * Get file content.
     * 
     * @param filePath Absolute file path.
     * @return File content.
     */
    public static byte[] getFileContent(String filePath) {
        try {
            return Files.readAllBytes(Path.of(filePath));
        } catch (IOException ioe) {
            throw new RuntimeException(format("File not found: %s", filePath));
        }
    }

    /**
     * Whether the {@link KafkaTopic} status has replicas change.
     * 
     * @param status Topic status.
     *               
     * @return True if there is replicas change status.
     */
    public static boolean hasReplicasChange(KafkaTopicStatus status) {
        return status != null && status.getReplicasChange() != null;
    }
    
    /**
     * Build Basic HTTP authentication header value.
     * 
     * @param username Username.
     * @param password Password.
     * 
     * @return Header value.
     */
    public static String buildBasicAuthValue(String username, String password) {
        String credentials = join(":", username, password);
        return format("Basic %s", Util.encodeToBase64(credentials));
    }
}
