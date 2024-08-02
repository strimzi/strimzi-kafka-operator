/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.cruisecontrol;

import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.ReplicasChangeStatusBuilder;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.topic.TopicOperatorConfig;
import io.strimzi.operator.topic.TopicOperatorUtil;
import io.strimzi.operator.topic.cruisecontrol.CruiseControlClient.TaskState;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.model.ReconcilableTopic;
import io.strimzi.operator.topic.model.Results;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.ONGOING;
import static io.strimzi.api.kafka.model.topic.ReplicasChangeState.PENDING;
import static io.strimzi.operator.topic.TopicOperatorUtil.hasReplicasChangeStatus;
import static io.strimzi.operator.topic.TopicOperatorUtil.topicNames;
import static java.lang.String.format;
import static org.apache.logging.log4j.core.util.Throwables.getRootCause;

/**
 * Handler for Cruise Control requests.
 */
public class CruiseControlHandler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CruiseControlHandler.class);
    
    private final TopicOperatorConfig config;
    private final TopicOperatorMetricsHolder metricsHolder;
    private final CruiseControlClient cruiseControlClient;

    /**
     * Create a new instance.
     *
     * @param config Topic Operator configuration.
     * @param metricsHolder Metrics holder.
     * @param cruiseControlClient Cruise Control client.
     */
    public CruiseControlHandler(TopicOperatorConfig config,
                                TopicOperatorMetricsHolder metricsHolder,
                                CruiseControlClient cruiseControlClient) {
        this.config = config;
        this.metricsHolder = metricsHolder;
        this.cruiseControlClient = cruiseControlClient;
    }

    /**
     * Send a topic_configuration request to create a task for replication factor change of one or more topics.
     * This should be called when one or more .spec.replicas changes are detected.
     * Note that this method also updates the KafkaTopic status.
     * 
     * @param reconcilableTopics Pending replicas changes.
     * @return Results with status updates.
     */
    public Results requestPendingChanges(List<ReconcilableTopic> reconcilableTopics) {
        var results = new Results();
        if (reconcilableTopics.isEmpty()) {
            return results;
        }
        results.addAll(updateToPending(reconcilableTopics, "Replicas change pending"));

        var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
        try {
            LOGGER.debugOp("Sending topic configuration request, topics {}", topicNames(reconcilableTopics));
            var kafkaTopics = reconcilableTopics.stream().map(ReconcilableTopic::kt).collect(Collectors.toList());
            var userTaskId = cruiseControlClient.topicConfiguration(kafkaTopics);
            results.addAll(updateToOngoing(reconcilableTopics, "Replicas change ongoing", userTaskId));
        } catch (Throwable t) {
            results.addAll(updateToFailed(reconcilableTopics, format("Replicas change failed, %s", getRootCause(t).getMessage())));
        }
        TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::cruiseControlTopicConfig, config.enableAdditionalMetrics(), config.namespace());
        
        return results;
    }

    /**
     * Send a user_tasks request to check the state of ongoing replication factor changes.
     * This should be called periodically to update the active tasks cache and KafkaTopic status.
     * Note that this method also updates the KafkaTopic status.
     *
     * @param reconcilableTopics Ongoing replicas changes.
     * @return Results with status updates.
     */
    public Results requestOngoingChanges(List<ReconcilableTopic> reconcilableTopics) {
        var results = new Results();
        if (reconcilableTopics.isEmpty()) {
            return results;
        }
        
        var groupByUserTaskId = reconcilableTopics.stream()
            .filter(rt -> hasReplicasChangeStatus(rt.kt()) && rt.kt().getStatus().getReplicasChange().getSessionId() != null)
            .map(rt -> new ReconcilableTopic(new Reconciliation("", KafkaTopic.RESOURCE_KIND, "", ""), rt.kt(), rt.topicName()))
            .collect(Collectors.groupingBy(rt -> rt.kt().getStatus().getReplicasChange().getSessionId(), HashMap::new, Collectors.toList()));

        var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
        try {
            LOGGER.debugOp("Sending user tasks request, Tasks {}", groupByUserTaskId.keySet());
            var userTasksResponse = cruiseControlClient.userTasks(groupByUserTaskId.keySet());
            if (userTasksResponse.userTasks().isEmpty()) {
                // Cruise Control restarted: reset the state because the tasks queue is not persisted
                // this may also happen when the tasks' retention time expires, or the cache becomes full
                results.addAll(updateToPending(reconcilableTopics, "Task not found, Resetting the state"));
            } else {
                for (var userTask : userTasksResponse.userTasks()) {
                    String userTaskId = userTask.userTaskId();
                    TaskState state = TaskState.get(userTask.status());
                    switch (state) {
                        case COMPLETED:
                            results.addAll(updateToCompleted(groupByUserTaskId.get(userTaskId), "Replicas change completed"));
                            break;
                        case COMPLETED_WITH_ERROR:
                            results.addAll(updateToFailed(groupByUserTaskId.get(userTaskId), "Replicas change completed with error"));
                            break;
                        case ACTIVE:
                        case IN_EXECUTION:
                            results.addAll(updateToOngoing(reconcilableTopics, "Replicas change ongoing", userTaskId));
                            break;
                    }
                }
            }
        } catch (Throwable t) {
            results.addAll(updateToFailed(reconcilableTopics, format("Replicas change failed, %s", getRootCause(t).getMessage())));
        }
        TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::cruiseControlUserTasks, config.enableAdditionalMetrics(), config.namespace());
        
        return results;
    }

    /**
     * Complete zombie changes.
     * These are pending but completed replication factor changes caused by Cruise Control restart or user revert.
     *
     * @param reconcilableTopics Pending but completed replicas changes.
     * @return Results with status updates.
     */
    public Results completeZombieChanges(List<ReconcilableTopic> reconcilableTopics) {
        return updateToCompleted(reconcilableTopics, "Replicas change completed or reverted");
    }
    
    private Results updateToPending(List<ReconcilableTopic> reconcilableTopics, String message) {
        var results = new Results();
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            results.setReplicasChange(reconcilableTopic, 
                new ReplicasChangeStatusBuilder()
                    .withState(PENDING)
                    .withTargetReplicas(reconcilableTopic.kt().getSpec().getReplicas())
                    .build())
        );
        return results;
    }
    
    private Results updateToOngoing(List<ReconcilableTopic> reconcilableTopics, String message, String userTaskId) {
        var results = new Results();
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic -> {
            var targetReplicas = hasReplicasChangeStatus(reconcilableTopic.kt())
                ? reconcilableTopic.kt().getStatus().getReplicasChange().getTargetReplicas()
                : reconcilableTopic.kt().getSpec().getReplicas();
            results.setReplicasChange(reconcilableTopic,
                new ReplicasChangeStatusBuilder()
                    .withState(ONGOING)
                    .withTargetReplicas(targetReplicas)
                    .withSessionId(userTaskId)
                    .build());
        });
        return results;
    }
    
    private Results updateToCompleted(List<ReconcilableTopic> reconcilableTopics, String message) {
        var results = new Results();
        LOGGER.infoOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic ->
            results.setReplicasChange(reconcilableTopic, null)
        );
        return results;
    }
    
    private Results updateToFailed(List<ReconcilableTopic> reconcilableTopics, String message) {
        var results = new Results();
        LOGGER.errorOp("{}, Topics: {}", message, topicNames(reconcilableTopics));
        reconcilableTopics.forEach(reconcilableTopic -> {
            var state = hasReplicasChangeStatus(reconcilableTopic.kt())
                ? reconcilableTopic.kt().getStatus().getReplicasChange().getState()
                : PENDING;
            var targetReplicas = hasReplicasChangeStatus(reconcilableTopic.kt())
                ? reconcilableTopic.kt().getStatus().getReplicasChange().getTargetReplicas()
                : reconcilableTopic.kt().getSpec().getReplicas();
            results.setReplicasChange(reconcilableTopic,
                new ReplicasChangeStatusBuilder()
                    .withState(state)
                    .withTargetReplicas(targetReplicas)
                    .withMessage(message)
                    .build());
        });
        return results;
    }
}
