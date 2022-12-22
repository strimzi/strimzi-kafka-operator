/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Annotations;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.Map;
import java.util.Objects;

/** Kubernetes Topic Watcher which is used to trigger reconciliation  */
class K8sTopicWatcher implements Watcher<KafkaTopic> {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(K8sTopicWatcher.class);
    private final Future<Void> initReconcileFuture;
    private final Runnable onHttpGoneTask;

    private TopicOperator topicOperator;

    /**
     * Constructor
     *
     * @param topicOperator  Instance of the Topic Operator
     * @param initReconcileFuture  Future of initial event for topic during initial reconcile
     * @param onHttpGoneTask  Runnable use to create/run the thread
     */
    public K8sTopicWatcher(TopicOperator topicOperator, Future<Void> initReconcileFuture, Runnable onHttpGoneTask) {
        this.topicOperator = topicOperator;
        this.initReconcileFuture = initReconcileFuture;
        this.onHttpGoneTask = onHttpGoneTask;
    }

    /**
     * Process Kubernetes events based on actions performed on KafkaTopic
     *
     * @param action      Kubernetes action performed
     * @param kafkaTopic  The Kafka topic resource
     */
    @Override
    public void eventReceived(Action action, KafkaTopic kafkaTopic) {
        ObjectMeta metadata = kafkaTopic.getMetadata();
        Map<String, String> labels = metadata.getLabels();
        if (kafkaTopic.getSpec() != null) {
            LogContext logContext = LogContext.kubeWatch(action, kafkaTopic).withKubeTopic(kafkaTopic);
            String name = metadata.getName();
            String kind = kafkaTopic.getKind();
            if (!initReconcileFuture.isComplete()) {
                LOGGER.debugCr(logContext.toReconciliation(), "Ignoring initial event for {} {} during initial reconcile", kind, name);
                return;
            }
            if (action.equals(Action.ERROR)) {
                LOGGER.errorCr(logContext.toReconciliation(), "Watch received action=ERROR for {} {} {}", kind, name, kafkaTopic);
            } else {
                PauseAnnotationChanges pauseAnnotationChanges = pausedAnnotationChanged(kafkaTopic);
                if (action.equals(Action.DELETED) || shouldReconcile(kafkaTopic, metadata, pauseAnnotationChanges.isChanged())) {
                    if (pauseAnnotationChanges.isResourcePausedByAnno()) {
                        topicOperator.pausedTopicCounter.getAndIncrement();
                    } else if (pauseAnnotationChanges.isResourceUnpausedByAnno()) {
                        topicOperator.pausedTopicCounter.getAndDecrement();
                    }
                    LOGGER.infoCr(logContext.toReconciliation(), "event {} on resource {} generation={}, labels={}", action, name,
                            metadata.getGeneration(), labels);
                    Handler<AsyncResult<Void>> resultHandler = ar -> {
                        if (ar.succeeded()) {
                            LOGGER.infoCr(logContext.toReconciliation(), "Success processing event {} on resource {} with labels {}", action, name, labels);
                        } else {
                            String message;
                            if (ar.cause() instanceof InvalidTopicException) {
                                message = kind + " " + name + " has an invalid spec section: " + ar.cause().getMessage();
                                LOGGER.errorCr(logContext.toReconciliation(), message);

                            } else {
                                message = "Failure processing " + kind + " watch event " + action + " on resource " + name + " with labels " + labels + ": " + ar.cause().getMessage();
                                LOGGER.errorCr(logContext.toReconciliation(), message, ar.cause());
                            }
                            topicOperator.enqueue(logContext, topicOperator.new Event(logContext, kafkaTopic, message, TopicOperator.EventType.WARNING, errorResult -> {
                            }));
                        }
                    };
                    topicOperator.onResourceEvent(logContext, kafkaTopic, action).onComplete(resultHandler);
                } else {
                    LOGGER.debugCr(logContext.toReconciliation(), "Ignoring {} to {} {} because metadata.generation==status.observedGeneration", action, kind, name);
                }
            }
        }
    }

    /**
     * Decides whether reconciliation is needed or not
     *
     * @param kafkaTopic              The Kafka topic resource
     * @param metadata                Object metadata
     * @param pauseAnnotationChanged  Pause the Kafka topic reconciliation or not
     * @return  Returns a boolean value based on whether reconciliation is required or not
     */
    public boolean shouldReconcile(KafkaTopic kafkaTopic, ObjectMeta metadata, boolean pauseAnnotationChanged) {
        return kafkaTopic.getStatus() == null // Not status => new KafkaTopic
                // KT has changed
                || !Objects.equals(metadata.getGeneration(), kafkaTopic.getStatus().getObservedGeneration())
                // changing just annotations does not increase the generation of resource, thus we need to check them
                || pauseAnnotationChanged;
    }

    /**
     * Check whether the paused annotation is changed in Kafka topic resource
     *
     * @param kafkaTopic              The Kafka topic resource
     * @return Returns paused annotation changes which depicts whether the annotation is now moved from unpaused to paused or paused to unpaused.
     */
    private PauseAnnotationChanges pausedAnnotationChanged(KafkaTopic kafkaTopic) {
        boolean pausedByAnno = Annotations.isReconciliationPausedWithAnnotation(kafkaTopic.getMetadata());
        boolean pausedInStatus = kafkaTopic.getStatus() != null && kafkaTopic.getStatus().getConditions().stream().anyMatch(condition -> "ReconciliationPaused".equals(condition.getType()));
        boolean wasUnpausedIsPaused = pausedByAnno && !pausedInStatus;
        boolean wasPausedIsUnpaused = !pausedByAnno && pausedInStatus;
        return new PauseAnnotationChanges(wasUnpausedIsPaused, wasPausedIsUnpaused);

    }

    /**
     * Close the topic watcher
     *
     * @param exception     Watcher Exception
     */
    @Override
    public void onClose(WatcherException exception) {
        LOGGER.debugOp("Closing {}", this);
        if (exception != null) {
            LOGGER.debugOp("Restarting  topic watcher due to ", exception);
            onHttpGoneTask.run();
        }
    }

    private static class PauseAnnotationChanges {
        private boolean resourcePausedByAnno;
        private boolean resourceUnpausedByAnno;
        private boolean isChanged;
        public PauseAnnotationChanges(boolean resourcePausedByAnno, boolean resourceUnpausedByAnno) {
            this.resourcePausedByAnno = resourcePausedByAnno;
            this.resourceUnpausedByAnno = resourceUnpausedByAnno;
            this.isChanged = this.resourcePausedByAnno || this.resourceUnpausedByAnno;
        }

        private boolean isResourcePausedByAnno() {
            return resourcePausedByAnno;
        }

        private boolean isResourceUnpausedByAnno() {
            return resourceUnpausedByAnno;
        }

        private boolean isChanged() {
            return isChanged;
        }
    }
}
