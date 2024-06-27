/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.micrometer.core.instrument.Timer;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatusBuilder;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatusBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient.TaskState;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient.UserTasksResponse;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.model.ReconcilableTopic;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.ONGOING;
import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.PENDING;
import static io.strimzi.operator.topic.TopicOperatorUtil.hasReplicasChange;
import static io.strimzi.operator.topic.TopicOperatorUtil.topicNames;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.logging.log4j.core.util.Throwables.getRootCause;

/**
 * Replicas change handler that interacts with Cruise Control REST API.
 * <br/><br/>
 * At any given time, a {@code .spec.replicas} change can be in one of the following states:
 * <ul><li>Pending: Not in Cruise Control's task queue (not yet sent or request error).</li>
 * <li>Ongoing: In Cruise Control's task queue, but execution not started, or not completed.</li>
 * <li>Completed: Cruise Control's task execution completed (target replication factor reconciled).</li></ul>
 */
public class ReplicasChangeHandler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ReplicasChangeHandler.class);
    
    private final TopicOperatorConfig config;
    private final TopicOperatorMetricsHolder metrics;
    private final CruiseControlClient cruiseControlClient;
    
    ReplicasChangeHandler(TopicOperatorConfig config,
                          TopicOperatorMetricsHolder metrics) {
        this(config, metrics, CruiseControlClient.create(
            config.cruiseControlHostname(),
            config.cruiseControlPort(),
            config.cruiseControlRackEnabled(),
            config.cruiseControlSslEnabled(),
            config.cruiseControlSslEnabled() ? getFileContent(config.cruiseControlCrtFilePath()) : null,
            config.cruiseControlAuthEnabled(),
            config.cruiseControlAuthEnabled() ? new String(getFileContent(config.cruiseControlApiUserPath()), UTF_8) : null,
            config.cruiseControlAuthEnabled() ? new String(getFileContent(config.cruiseControlApiPassPath()), UTF_8) : null
        ));
    }
    
    ReplicasChangeHandler(TopicOperatorConfig config,
                          TopicOperatorMetricsHolder metrics, 
                          CruiseControlClient cruiseControlClient) {
        this.config = config;
        this.metrics = metrics;
        this.cruiseControlClient = cruiseControlClient;
    }

    /**
     * Stop the replicas change handler.
     */
    public void stop() {
        try {
            cruiseControlClient.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Send a topic_configuration request to create a task for replication factor change of one or more topics.
     * This should be called when one or more .spec.replicas changes are detected.
     * Note that this method also updates the KafkaTopic status.
     * 
     * @param reconcilableTopics Pending replicas changes.
     * @return Replicas changes with status update.
     */
    public List<ReconcilableTopic> requestPendingChanges(List<ReconcilableTopic> reconcilableTopics) {
        List<ReconcilableTopic> result = new ArrayList<>();
        if (reconcilableTopics.isEmpty()) {
            return result;
        }
        updateToPending(reconcilableTopics, "Replicas change pending");
        result.addAll(reconcilableTopics);

        Timer.Sample timerSample = TopicOperatorUtil.startExternalRequestTimer(metrics, config.enableAdditionalMetrics());
        try {
            LOGGER.debugOp("Sending topic configuration request, topics {}", topicNames(reconcilableTopics));
            List<KafkaTopic> kafkaTopics = reconcilableTopics.stream().map(rt -> rt.kt()).collect(Collectors.toList());
            String userTaskId = cruiseControlClient.topicConfiguration(kafkaTopics);
            updateToOngoing(result, "Replicas change ongoing", userTaskId);
        } catch (Throwable t) {
            updateToFailed(result, format("Replicas change failed, %s", getRootCause(t).getMessage()));
        }
        TopicOperatorUtil.stopExternalRequestTimer(timerSample, metrics::cruiseControlTopicConfig, config.enableAdditionalMetrics(), config.namespace());
        return result;
    }

    /**
     * Send a user_tasks request to check the state of ongoing replication factor changes.
     * This should be called periodically to update the active tasks cache and KafkaTopic status.
     * Note that this method also updates the KafkaTopic status.
     *
     * @param reconcilableTopics Ongoing replicas changes.
     * @return Replicas changes with status update.
     */
    public List<ReconcilableTopic> requestOngoingChanges(List<ReconcilableTopic> reconcilableTopics) {
        List<ReconcilableTopic> result = new ArrayList<>();
        if (reconcilableTopics.isEmpty()) {
            return result;
        }
        result.addAll(reconcilableTopics);
        
        Map<String, List<ReconcilableTopic>> groupByUserTaskId = reconcilableTopics.stream()
            .filter(rt -> hasReplicasChange(rt.kt().getStatus()) && rt.kt().getStatus().getReplicasChange().getSessionId() != null)
            .map(rt -> new ReconcilableTopic(new Reconciliation("", KafkaTopic.RESOURCE_KIND, "", ""), rt.kt(), rt.topicName()))
            .collect(Collectors.groupingBy(rt -> rt.kt().getStatus().getReplicasChange().getSessionId(), HashMap::new, Collectors.toList()));

        Timer.Sample timerSample = TopicOperatorUtil.startExternalRequestTimer(metrics, config.enableAdditionalMetrics());
        try {
            LOGGER.debugOp("Sending user tasks request, Tasks {}", groupByUserTaskId.keySet());
            UserTasksResponse utr = cruiseControlClient.userTasks(groupByUserTaskId.keySet());
            if (utr.userTasks().isEmpty()) {
                // Cruise Control restarted: reset the state because the tasks queue is not persisted
                // this may also happen when the tasks' retention time expires, or the cache becomes full
                updateToPending(result, "Task not found, Resetting the state");
            } else {
                for (var userTask : utr.userTasks()) {
                    String userTaskId = userTask.userTaskId();
                    TaskState state = TaskState.get(userTask.status());
                    switch (state) {
                        case COMPLETED:
                            updateToCompleted(groupByUserTaskId.get(userTaskId), "Replicas change completed");
                            break;
                        case COMPLETED_WITH_ERROR:
                            updateToFailed(groupByUserTaskId.get(userTaskId), "Replicas change completed with error");
                            break;
                        case ACTIVE:
                        case IN_EXECUTION:
                            // do nothing
                            break;
                    }
                }
            }
        } catch (Throwable t) {
            updateToFailed(result, format("Replicas change failed, %s", getRootCause(t).getMessage()));
        }
        TopicOperatorUtil.stopExternalRequestTimer(timerSample, metrics::cruiseControlUserTasks, config.enableAdditionalMetrics(), config.namespace());
        return result;
    }
    
    private static byte[] getFileContent(String filePath) {
        try {
            return Files.readAllBytes(Path.of(filePath));
        } catch (IOException ioe) {
            throw new UncheckedIOException(format("File not found: %s", filePath), ioe);
        }
    }

    private void updateToPending(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withReplicasChange(new ReplicasChangeStatusBuilder()
                    .withState(PENDING).withTargetReplicas(reconcilableTopic.kt().getSpec().getReplicas()).build()).build()));
    }
    
    private void updateToOngoing(List<ReconcilableTopic> reconcilableTopics, String message, String userTaskId) {
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .editOrNewReplicasChange().withState(ONGOING).withSessionId(userTaskId).endReplicasChange().build()));
    }
    
    private void updateToCompleted(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .withReplicasChange(null).build()));
    }
    
    private void updateToFailed(List<ReconcilableTopic> reconcilableTopics, String message) {
        LOGGER.errorOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            reconcilableTopic.kt().setStatus(new KafkaTopicStatusBuilder(reconcilableTopic.kt().getStatus())
                .editOrNewReplicasChange().withMessage(message).endReplicasChange().build()));
    }
}
