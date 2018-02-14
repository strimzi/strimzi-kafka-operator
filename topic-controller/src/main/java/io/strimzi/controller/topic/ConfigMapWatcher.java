/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class ConfigMapWatcher implements Watcher<ConfigMap> {

    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigMapWatcher.class);

    private Controller controller;
    private final LabelPredicate cmPredicate;

    public ConfigMapWatcher(Controller controller, LabelPredicate cmPredicate) {
        this.controller = controller;
        this.cmPredicate = cmPredicate;
    }

    public void eventReceived(Action action, ConfigMap configMap) {
        ObjectMeta metadata = configMap.getMetadata();
        Map<String, String> labels = metadata.getLabels();
        if (cmPredicate.test(configMap)) {
            String name = metadata.getName();
            LOGGER.info("ConfigMap watch received event {} on map {} with labels {}", action, name, labels);
            Handler<AsyncResult<Void>> resultHandler = ar -> {
                if (ar.succeeded()) {
                    LOGGER.info("Success processing ConfigMap watch event {} on map {} with labels {}", action, name, labels);
                } else {
                    String message;
                    if (ar.cause() instanceof InvalidConfigMapException) {
                        message = "ConfigMap " + name + " has an invalid 'data' section: " + ar.cause().getMessage();
                        LOGGER.error("{}", message);

                    } else {
                        message = "Failure processing ConfigMap watch event " + action + " on map " + name + " with labels " + labels + ": " + ar.cause().getMessage();
                        LOGGER.error("{}", message, ar.cause());
                    }
                    controller.enqueue(controller.new Event(configMap, message, Controller.EventType.WARNING, errorResult -> { }));
                }
            };
            switch (action) {
                case ADDED:
                    controller.onConfigMapAdded(configMap, resultHandler);
                    break;
                case MODIFIED:
                    controller.onConfigMapModified(configMap, resultHandler);
                    break;
                case DELETED:
                    controller.onConfigMapDeleted(configMap, resultHandler);
                    break;
                case ERROR:
                    LOGGER.error("Watch received action=ERROR for ConfigMap " + name);
            }
        }
    }

    public void onClose(KubernetesClientException e) {
        LOGGER.debug("Closing {}", this);
    }
}
