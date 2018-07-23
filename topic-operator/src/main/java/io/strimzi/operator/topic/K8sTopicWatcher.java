/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

class K8sTopicWatcher implements Watcher<KafkaTopic> {

    private final static Logger LOGGER = LogManager.getLogger(K8sTopicWatcher.class);

    private TopicOperator topicOperator;
    private final LabelPredicate resourcePredicate;

    public K8sTopicWatcher(TopicOperator topicOperator, LabelPredicate resourcePredicate) {
        this.topicOperator = topicOperator;
        this.resourcePredicate = resourcePredicate;
    }

    @Override
    public void eventReceived(Action action, KafkaTopic kafkaTopic) {
        ObjectMeta metadata = kafkaTopic.getMetadata();
        Map<String, String> labels = metadata.getLabels();
        if (resourcePredicate.test(kafkaTopic)) {
            String name = metadata.getName();
            String kind = kafkaTopic.getKind();
            LOGGER.info(kind + " watch received event {} on resource {} with labels {}", action, name, labels);
            Handler<AsyncResult<Void>> resultHandler = ar -> {
                if (ar.succeeded()) {
                    LOGGER.info("Success processing KafkaTopic watch event {} on resource {} with labels {}", action, name, labels);
                } else {
                    String message;
                    if (ar.cause() instanceof InvalidTopicException) {
                        message = kind + " " + name + " has an invalid spec section: " + ar.cause().getMessage();
                        LOGGER.error("{}", message);

                    } else {
                        message = "Failure processing " + kind + " watch event " + action + " on resource " + name + " with labels " + labels + ": " + ar.cause().getMessage();
                        LOGGER.error("{}", message, ar.cause());
                    }
                    topicOperator.enqueue(topicOperator.new Event(kafkaTopic, message, TopicOperator.EventType.WARNING, errorResult -> { }));
                }
            };
            switch (action) {
                case ADDED:
                    topicOperator.onResourceAdded(kafkaTopic, resultHandler);
                    break;
                case MODIFIED:
                    topicOperator.onResourceModified(kafkaTopic, resultHandler);
                    break;
                case DELETED:
                    topicOperator.onResourceDeleted(kafkaTopic, resultHandler);
                    break;
                case ERROR:
                    LOGGER.error("Watch received action=ERROR for {} {}", kind, name);
            }
        }
    }

    @Override
    public void onClose(KubernetesClientException e) {
        LOGGER.debug("Closing {}", this);
    }
}
