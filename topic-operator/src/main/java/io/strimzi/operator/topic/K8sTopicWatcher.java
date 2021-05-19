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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;

class K8sTopicWatcher implements Watcher<KafkaTopic> {

    private final static Logger LOGGER = LogManager.getLogger(K8sTopicWatcher.class);
    private final static ReconciliationLogger RECONCILIATION_LOGGER = ReconciliationLogger.create(LOGGER);
    private final Future<Void> initReconcileFuture;
    private final Runnable onHttpGoneTask;

    private TopicOperator topicOperator;

    public K8sTopicWatcher(TopicOperator topicOperator, Future<Void> initReconcileFuture, Runnable onHttpGoneTask) {
        this.topicOperator = topicOperator;
        this.initReconcileFuture = initReconcileFuture;
        this.onHttpGoneTask = onHttpGoneTask;
    }

    @Override
    public void eventReceived(Action action, KafkaTopic kafkaTopic) {
        ObjectMeta metadata = kafkaTopic.getMetadata();
        Map<String, String> labels = metadata.getLabels();
        if (kafkaTopic.getSpec() != null) {
            LogContext logContext = LogContext.kubeWatch(action, kafkaTopic).withKubeTopic(kafkaTopic);
            String name = metadata.getName();
            String kind = kafkaTopic.getKind();
            if (!initReconcileFuture.isComplete()) {
                RECONCILIATION_LOGGER.debug(logContext.toReconciliation(), "Ignoring initial event for {} {} during initial reconcile", kind, name);
                return;
            }
            if (action.equals(Action.ERROR)) {
                RECONCILIATION_LOGGER.error(logContext.toReconciliation(), "Watch received action=ERROR for {} {} {}", kind, name, kafkaTopic);
            } else {
                PauseAnnotationChanges pauseAnnotationChanges = pausedAnnotationChanged(kafkaTopic);
                if (action.equals(Action.DELETED) || shouldReconcile(kafkaTopic, metadata, pauseAnnotationChanges.isChanged())) {
                    if (pauseAnnotationChanges.isResourcePausedByAnno()) {
                        topicOperator.pausedTopicCounter.getAndIncrement();
                    } else if (pauseAnnotationChanges.isResourceUnpausedByAnno()) {
                        topicOperator.pausedTopicCounter.getAndDecrement();
                    }
                    RECONCILIATION_LOGGER.info(logContext.toReconciliation(), "event {} on resource {} generation={}, labels={}", action, name,
                            metadata.getGeneration(), labels);
                    Handler<AsyncResult<Void>> resultHandler = ar -> {
                        if (ar.succeeded()) {
                            RECONCILIATION_LOGGER.info(logContext.toReconciliation(), "Success processing event {} on resource {} with labels {}", action, name, labels);
                        } else {
                            String message;
                            if (ar.cause() instanceof InvalidTopicException) {
                                message = kind + " " + name + " has an invalid spec section: " + ar.cause().getMessage();
                                RECONCILIATION_LOGGER.error(logContext.toReconciliation(), message);

                            } else {
                                message = "Failure processing " + kind + " watch event " + action + " on resource " + name + " with labels " + labels + ": " + ar.cause().getMessage();
                                RECONCILIATION_LOGGER.error(logContext.toReconciliation(), message, ar.cause());
                            }
                            topicOperator.enqueue(logContext, topicOperator.new Event(logContext, kafkaTopic, message, TopicOperator.EventType.WARNING, errorResult -> {
                            }));
                        }
                    };
                    topicOperator.onResourceEvent(logContext, kafkaTopic, action).onComplete(resultHandler);
                } else {
                    RECONCILIATION_LOGGER.debug(logContext.toReconciliation(), "Ignoring {} to {} {} because metadata.generation==status.observedGeneration", action, kind, name);
                }
            }
        }
    }

    public boolean shouldReconcile(KafkaTopic kafkaTopic, ObjectMeta metadata, boolean pauseAnnotationChanged) {
        return kafkaTopic.getStatus() == null // Not status => new KafkaTopic
                // KT has changed
                || !Objects.equals(metadata.getGeneration(), kafkaTopic.getStatus().getObservedGeneration())
                // changing just annotations does not increase the generation of resource, thus we need to check them
                || pauseAnnotationChanged;
    }

    private PauseAnnotationChanges pausedAnnotationChanged(KafkaTopic kafkaTopic) {
        boolean pausedByAnno = Annotations.isReconciliationPausedWithAnnotation(kafkaTopic.getMetadata());
        boolean pausedInStatus = kafkaTopic.getStatus() != null && kafkaTopic.getStatus().getConditions().stream().filter(condition -> "ReconciliationPaused".equals(condition.getType())).findAny().isPresent();
        boolean wasUnpausedIsPaused = pausedByAnno && !pausedInStatus;
        boolean wasPausedIsUnpaused = !pausedByAnno && pausedInStatus;
        return new PauseAnnotationChanges(wasUnpausedIsPaused, wasPausedIsUnpaused);

    }


    @Override
    public void onClose(WatcherException exception) {
        LOGGER.debug("Closing {}", this);
        if (exception != null) {
            LOGGER.debug("Restarting  topic watcher due to ", exception);
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

        public boolean isResourcePausedByAnno() {
            return resourcePausedByAnno;
        }

        public boolean isResourceUnpausedByAnno() {
            return resourceUnpausedByAnno;
        }

        public boolean isChanged() {
            return isChanged;
        }
    }
}
