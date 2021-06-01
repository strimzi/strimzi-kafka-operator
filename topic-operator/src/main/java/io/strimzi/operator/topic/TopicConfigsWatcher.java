/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

/**
 * ZooKeeper watcher for child znodes of {@code /configs/topics},
 * calling {@link TopicOperator#onTopicConfigChanged(LogContext, TopicName)}
 * for changed children.
 */
class TopicConfigsWatcher extends ZkWatcher {

    TopicConfigsWatcher(TopicOperator topicOperator) {
        super(topicOperator, CONFIGS_ZNODE);
    }

    @Override
    protected void notifyOperator(String child) {
        LogContext logContext = LogContext.zkWatch(CONFIGS_ZNODE, "=" + child, topicOperator.getNamespace(), child);
        logger.infoCr(logContext.toReconciliation(), "Topic config change");
        topicOperator.onTopicConfigChanged(logContext, new TopicName(child)).onComplete(ar2 -> {
            logger.infoCr(logContext.toReconciliation(), "Reconciliation result due to topic config change on topic {}: {}", child, ar2);
        });
    }
}
