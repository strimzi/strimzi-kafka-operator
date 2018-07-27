/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.vertx.core.Handler;

/**
 * ZooKeeper watcher for child znodes of {@code /brokers/topics},
 * calling {@link TopicOperator#onTopicPartitionsChanged(TopicName, Handler)}
 * for changed children.
 */
public class ZkTopicWatcher extends ZkWatcher {

    private static final String TOPICS_ZNODE = "/brokers/topics";

    ZkTopicWatcher(TopicOperator topicOperator) {
        super(topicOperator, TOPICS_ZNODE);
    }

    @Override
    protected void notifyOperator(String child) {
        log.debug("Partitions change for topic {}", child);
        topicOperator.onTopicPartitionsChanged(new TopicName(child), ar -> {
            log.info("Reconciliation result due to topic partitions change: {}", ar);
        });
    }
}
