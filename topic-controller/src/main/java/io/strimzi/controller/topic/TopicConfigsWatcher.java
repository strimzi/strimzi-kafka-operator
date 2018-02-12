/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.vertx.core.Handler;

/**
 * ZooKeeper watcher for child znodes of {@code /configs/topics},
 * calling {@link Controller#onTopicConfigChanged(TopicName, Handler)}
 * for changed children.
 */
class TopicConfigsWatcher extends ZkWatcher {

    private static final String CONFIGS_ZNODE = "/config/topics";

    TopicConfigsWatcher(Controller controller) {
        super(controller, CONFIGS_ZNODE);
    }

    @Override
    protected void notifyController(String child) {
        log.debug("Config change for topic {}", child);
        controller.onTopicConfigChanged(new TopicName(child), ar2 -> {
            log.info("Reconciliation result due to topic config change: {}", ar2);
        });
    }
}
