/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

class K8sTopicWatcher implements Watcher<KafkaTopic> {

    private final static Logger LOGGER = LogManager.getLogger(K8sTopicWatcher.class);
    private final Future<Void> initReconcileFuture;

    private TopicOperator topicOperator;

    public K8sTopicWatcher(TopicOperator topicOperator, Future<Void> initReconcileFuture) {
        this.topicOperator = topicOperator;
        this.initReconcileFuture = initReconcileFuture;
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
                LOGGER.debug("Ignoring initial event for {} {} during initial reconcile", kind, name);
                return;
            }
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
                    topicOperator.enqueue(topicOperator.new Event(kafkaTopic, message, TopicOperator.EventType.WARNING, errorResult -> { }));
                }
            };
            if (!action.equals(Action.ERROR)) {
                topicOperator.onResourceEvent(logContext, kafkaTopic, action).onComplete(resultHandler);
            } else {
                LOGGER.error("Watch received action=ERROR for {} {}", kind, name);
            }
        }
    }

    @Override
    public void onClose(KubernetesClientException e) {
        LOGGER.debug("Closing {}", this);
    }
}
