/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.vertx.core.Handler;

/**
 * ZooKeeper watcher for child znodes of {@code /brokers/topics},
 * calling {@link Controller#onTopicPartitionsChanged(TopicName, Handler)}
 * for changed children.
 */
public class TopicWatcher extends ZkWatcher {

    private static final String TOPICS_ZNODE = "/brokers/topics";

    TopicWatcher(Controller controller) {
        super(controller, TOPICS_ZNODE);
    }

    @Override
    protected void notifyController(String child) {
        log.debug("Partitions change for topic {}", child);
        controller.onTopicPartitionsChanged(new TopicName(child), ar -> {
            log.info("Reconciliation result due to topic partitions change: {}", ar);
        });
    }
}
