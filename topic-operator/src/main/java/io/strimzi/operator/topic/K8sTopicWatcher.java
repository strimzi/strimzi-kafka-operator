/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.Annotations;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;

import static io.strimzi.operator.common.Annotations.ANNO_STRIMZI_IO_PAUSE_RECONCILIATION;

class K8sTopicWatcher implements Watcher<KafkaTopic> {

    private final static Logger LOGGER = LogManager.getLogger(K8sTopicWatcher.class);
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
        if (kafkaTopic.getMetadata() != null && kafkaTopic.getMetadata().getAnnotations() != null) {
            LOGGER.info("k8s mam anotaci {}", hasPauseReconciliationAnnotation(kafkaTopic));
        }
        if (kafkaTopic.getSpec() != null) {
            LogContext logContext = LogContext.kubeWatch(action, kafkaTopic).withKubeTopic(kafkaTopic);
            String name = metadata.getName();
            String kind = kafkaTopic.getKind();
            if (!initReconcileFuture.isComplete()) {
                LOGGER.debug("Ignoring initial event for {} {} during initial reconcile", kind, name);
                return;
            }
            if (action.equals(Action.ERROR)) {
                LOGGER.error("{}: Watch received action=ERROR for {} {} {}", logContext, kind, name, kafkaTopic);
            } else {
                if (action.equals(Action.DELETED) || shouldReconcile(kafkaTopic, metadata)) {
                    LOGGER.info("{}: event {} on resource {} generation={}, labels={}", logContext, action, name,
                            metadata.getGeneration(), labels);
                    Handler<AsyncResult<Void>> resultHandler = ar -> {
                        if (ar.succeeded()) {
                            LOGGER.info("{}: Success processing event {} on resource {} with labels {}", logContext, action, name, labels);
                        } else {
                            String message;
                            if (ar.cause() instanceof InvalidTopicException) {
                                message = kind + " " + name + " has an invalid spec section: " + ar.cause().getMessage();
                                LOGGER.error("{}", message);

                            } else {
                                message = "Failure processing " + kind + " watch event " + action + " on resource " + name + " with labels " + labels + ": " + ar.cause().getMessage();
                                LOGGER.error("{}: {}", logContext, message, ar.cause());
                            }
                            topicOperator.enqueue(topicOperator.new Event(kafkaTopic, message, TopicOperator.EventType.WARNING, errorResult -> {
                            }));
                        }
                    };
                    topicOperator.onResourceEvent(logContext, kafkaTopic, action).onComplete(resultHandler);
                } else {
                    LOGGER.debug("{}: Ignoring {} to {} {} because metadata.generation==status.observedGeneration", logContext, action, kind, name);
                }
            }
        }
    }

    protected boolean hasPauseReconciliationAnnotation(CustomResource resource) {
        return Annotations.booleanAnnotation(resource, ANNO_STRIMZI_IO_PAUSE_RECONCILIATION, false);
    }

    public boolean shouldReconcile(KafkaTopic kafkaTopic, ObjectMeta metadata) {
        return kafkaTopic.getStatus() == null // Not status => new KafkaTopic
                || !Objects.equals(metadata.getGeneration(), kafkaTopic.getStatus().getObservedGeneration()); // KT has changed
    }

    @Override
    public void onClose(WatcherException exception) {
        LOGGER.debug("Closing {}", this);
        if (exception != null) {
            LOGGER.debug("Restarting  topic watcher due to ", exception);
            onHttpGoneTask.run();
        }
    }
}
