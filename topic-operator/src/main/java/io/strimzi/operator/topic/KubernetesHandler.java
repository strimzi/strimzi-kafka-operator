/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;
import io.strimzi.api.kafka.model.topic.KafkaTopicStatus;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.topic.metrics.TopicOperatorMetricsHolder;
import io.strimzi.operator.topic.model.ReconcilableTopic;

import java.util.List;
import java.util.Objects;

/**
 * Handler for Kubernetes requests.
 */
public class KubernetesHandler {
    static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KubernetesHandler.class);
    
    /** Annotation for managing and unmanaging a KafkaTopic. */
    public static final String ANNO_STRIMZI_IO_MANAGED = "strimzi.io/managed";
    static final String FINALIZER_STRIMZI_IO_TO = "strimzi.io/topic-operator";
    
    private final TopicOperatorConfig config;
    private final TopicOperatorMetricsHolder metricsHolder;
    private final KubernetesClient kubernetesClient;

    /**
     * Create a new instance.
     *
     * @param config Topic Operator configuration.
     * @param metricsHolder Metrics holder.
     * @param kubernetesClient Kubernetes client.
     */
    KubernetesHandler(TopicOperatorConfig config, TopicOperatorMetricsHolder metricsHolder, KubernetesClient kubernetesClient) {
        this.config = config;
        this.metricsHolder = metricsHolder;
        this.kubernetesClient = kubernetesClient;
    }

    /**
     * Add finalizer to a KafkaTopic resource.
     * 
     * @param reconcilableTopic Reconcilable topic.
     * @return KafkaTopic resource.
     */
    public KafkaTopic addFinalizer(ReconcilableTopic reconcilableTopic) {
        if (!reconcilableTopic.kt().getMetadata().getFinalizers().contains(FINALIZER_STRIMZI_IO_TO)) {
            LOGGER.traceCr(reconcilableTopic.reconciliation(), "Adding finalizer {}", FINALIZER_STRIMZI_IO_TO);
            var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
            var withFinalizer = Crds.topicOperation(kubernetesClient).resource(reconcilableTopic.kt()).edit(old ->
                new KafkaTopicBuilder(old).editOrNewMetadata().addToFinalizers(FINALIZER_STRIMZI_IO_TO).endMetadata().build());
            TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::addFinalizerTimer, config.enableAdditionalMetrics(), config.namespace());
            LOGGER.traceCr(reconcilableTopic.reconciliation(), "Added finalizer {}, resourceVersion now {}", FINALIZER_STRIMZI_IO_TO, TopicOperatorUtil.resourceVersion(withFinalizer));
            return withFinalizer;
        }
        return reconcilableTopic.kt();
    }

    /**
     * Remove finalizer from a KafkaTopic resource.
     *
     * @param reconcilableTopic Reconcilable topic.
     * @return KafkaTopic resource.
     */
    public KafkaTopic removeFinalizer(ReconcilableTopic reconcilableTopic) {
        if (reconcilableTopic.kt().getMetadata().getFinalizers().contains(FINALIZER_STRIMZI_IO_TO)) {
            LOGGER.traceCr(reconcilableTopic.reconciliation(), "Removing finalizer {}", FINALIZER_STRIMZI_IO_TO);
            var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
            var withoutFinalizer = Crds.topicOperation(kubernetesClient).resource(reconcilableTopic.kt()).edit(old ->
                new KafkaTopicBuilder(old).editOrNewMetadata().removeFromFinalizers(FINALIZER_STRIMZI_IO_TO).endMetadata().build());
            TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::removeFinalizerTimer, config.enableAdditionalMetrics(), config.namespace());
            LOGGER.traceCr(reconcilableTopic.reconciliation(), "Removed finalizer {}, resourceVersion now {}", FINALIZER_STRIMZI_IO_TO, TopicOperatorUtil.resourceVersion(withoutFinalizer));
            return withoutFinalizer;
        } else {
            return reconcilableTopic.kt();
        }
    }

    /**
     * Update the KafkaTopic status.
     * 
     * @param reconcilableTopic Reconcilable topic.
     * @return KafkaTopic resource.
     */
    public KafkaTopic updateStatus(ReconcilableTopic reconcilableTopic) {
        var oldStatus = Crds.topicOperation(kubernetesClient)
            .inNamespace(reconcilableTopic.kt().getMetadata().getNamespace())
            .withName(reconcilableTopic.kt().getMetadata().getName()).get().getStatus();
        if (statusChanged(reconcilableTopic.kt(), oldStatus)) {
            // the observedGeneration is initialized to 0 when creating a paused topic (oldStatus null, paused true)
            // this will result in metadata.generation: 1 > status.observedGeneration: 0 (not reconciled)
            reconcilableTopic.kt().getStatus().setObservedGeneration(reconcilableTopic.kt().getStatus() != null && oldStatus != null
                ? !TopicOperatorUtil.isPaused(reconcilableTopic.kt()) ? reconcilableTopic.kt().getMetadata().getGeneration() : oldStatus.getObservedGeneration()
                : !TopicOperatorUtil.isPaused(reconcilableTopic.kt()) ? reconcilableTopic.kt().getMetadata().getGeneration() : 0L);
            reconcilableTopic.kt().getStatus().setTopicName(!TopicOperatorUtil.isManaged(reconcilableTopic.kt()) ? null
                : oldStatus != null && oldStatus.getTopicName() != null ? oldStatus.getTopicName()
                : TopicOperatorUtil.topicName(reconcilableTopic.kt()));
            var update = new KafkaTopicBuilder(reconcilableTopic.kt())
                .editOrNewMetadata()
                    .withResourceVersion(null)
                .endMetadata()
                .withStatus(reconcilableTopic.kt().getStatus())
                .build();
            LOGGER.traceCr(reconcilableTopic.reconciliation(), "Updating status with {}", update.getStatus());
            var timerSample = TopicOperatorUtil.startExternalRequestTimer(metricsHolder, config.enableAdditionalMetrics());
            try {
                var updated = Crds.topicOperation(kubernetesClient).resource(update).updateStatus();
                TopicOperatorUtil.stopExternalRequestTimer(timerSample, metricsHolder::updateStatusTimer, config.enableAdditionalMetrics(), config.namespace());
                LOGGER.traceCr(reconcilableTopic.reconciliation(), "Updated status to observedGeneration {}, resourceVersion {}",
                    updated.getStatus().getObservedGeneration(), updated.getMetadata().getResourceVersion());
                return updated;
            } catch (Throwable e) {
                LOGGER.errorOp("Status update failed: {}", e.getMessage());
            }
        }
        return reconcilableTopic.kt();
    }

    private boolean statusChanged(KafkaTopic kt, KafkaTopicStatus oldStatus) {
        return oldStatusOrTopicNameMissing(oldStatus)
            || nonPausedAndDifferentGenerations(kt, oldStatus)
            || differentConditions(kt.getStatus().getConditions(), oldStatus.getConditions())
            || replicasChangeDiffer(kt, oldStatus);
    }

    private boolean oldStatusOrTopicNameMissing(KafkaTopicStatus oldStatus) {
        return oldStatus == null || oldStatus.getTopicName() == null;
    }

    private boolean nonPausedAndDifferentGenerations(KafkaTopic kt, KafkaTopicStatus oldStatus) {
        return !TopicOperatorUtil.isPaused(kt) && oldStatus.getObservedGeneration() != kt.getMetadata().getGeneration();
    }

    private boolean differentConditions(List<Condition> newConditions, List<Condition> oldConditions) {
        if (Objects.equals(newConditions, oldConditions)) {
            return false;
        } else if (newConditions == null || oldConditions == null || newConditions.size() != oldConditions.size()) {
            return true;
        } else {
            for (int i = 0; i < newConditions.size(); i++) {
                if (conditionsDiffer(newConditions.get(i), oldConditions.get(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean conditionsDiffer(Condition newCondition, Condition oldCondition) {
        return !Objects.equals(newCondition.getType(), oldCondition.getType())
            || !Objects.equals(newCondition.getStatus(), oldCondition.getStatus())
            || !Objects.equals(newCondition.getReason(), oldCondition.getReason())
            || !Objects.equals(newCondition.getMessage(), oldCondition.getMessage());
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    private boolean replicasChangeDiffer(KafkaTopic kt, KafkaTopicStatus oldStatus) {
        var newReplicasChangeStatus = kt.getStatus().getReplicasChange();
        var oldReplicasChangeStatus = oldStatus.getReplicasChange();
        return newReplicasChangeStatus == null && oldReplicasChangeStatus != null
            || newReplicasChangeStatus != null && oldReplicasChangeStatus == null
            || (newReplicasChangeStatus != null && oldReplicasChangeStatus != null
                && !Objects.equals(newReplicasChangeStatus, oldReplicasChangeStatus));
    }
}
