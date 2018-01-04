/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    private final static Logger logger = LoggerFactory.getLogger(ConfigMapWatcher.class);

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
            logger.info("ConfigMap watch received event {} on map {} with labels {}", action, name, labels);
            Handler<AsyncResult<Void>> resultHandler = ar -> {
                if (ar.succeeded()) {
                    logger.info("Success processing ConfigMap watch event {} on map {} with labels {}", action, name, labels);
                } else {
                    logger.error("Failure processing ConfigMap watch event {} on map {} with labels {}", action, name, labels, ar.cause());
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
                    logger.error("Watch received action=ERROR for ConfigMap " + name);
            }
        }
    }

    public void onClose(KubernetesClientException e) {
        logger.debug("Closing {}", this);
    }
}
