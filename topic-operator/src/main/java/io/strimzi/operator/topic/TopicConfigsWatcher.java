/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.vertx.core.Handler;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * ZooKeeper watcher for child znodes of {@code /configs/topics},
 * calling {@link TopicOperator#onTopicConfigChanged(TopicName, Handler)}
 * for changed children.
 */
class TopicConfigsWatcher extends ZkWatcher {

    private static final String CONFIGS_ZNODE = "/config/topics";

    private AtomicInteger notification = new AtomicInteger();

    TopicConfigsWatcher(TopicOperator topicOperator) {
        super(topicOperator, CONFIGS_ZNODE);
    }

    @Override
    protected void notifyOperator(String child) {
        int notification = this.notification.getAndIncrement();
        log.info("Config change {}: topic {}", notification, child);
        topicOperator.onTopicConfigChanged(new TopicName(child), ar2 -> {
            log.info("Reconciliation result due to topic config change {} on topic {}: {}", notification, child, ar2);
        });
    }
}
