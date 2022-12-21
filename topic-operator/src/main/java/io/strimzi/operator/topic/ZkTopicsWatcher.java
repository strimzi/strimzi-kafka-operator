/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ZooKeeper watcher for child znodes of {@code /brokers/topics},
 * calling {@link TopicOperator#onTopicCreated(LogContext, TopicName)} for new children and
 * {@link TopicOperator#onTopicDeleted(LogContext, TopicName)} for deleted children.
 */
class ZkTopicsWatcher {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(ZkTopicsWatcher.class);

    private static final String TOPICS_ZNODE = "/brokers/topics";

    private final TopicOperator topicOperator;
    private final TopicConfigsWatcher tcw;
    private final ZkTopicWatcher tw;

    private List<String> children;

    private volatile int state = 0;

    /**
     * Constructor
     *
     * @param topicOperator    Operator instance
     * @param tcw   watcher for the topics config changes
     * @param tw    watcher for the topics partitions changes
     */
    ZkTopicsWatcher(TopicOperator topicOperator, TopicConfigsWatcher tcw, ZkTopicWatcher tw) {
        this.topicOperator = topicOperator;
        this.tcw = tcw;
        this.tw = tw;
    }

    void stop() {
        this.tcw.stop();
        this.tw.stop();
        this.state = 2;
    }

    boolean started() {
        return this.state == 1;
    }

    void start(Zk zk) {
        synchronized (this) {
            children = null;
        }
        tcw.start(zk);
        tw.start(zk);
        zk.watchChildren(TOPICS_ZNODE, new ChildrenWatchHandler(zk)).<Void>compose(zk2 -> {
            zk.children(TOPICS_ZNODE, childResult -> {
                if (childResult.failed()) {
                    LOGGER.errorOp("Error on znode {} children", TOPICS_ZNODE, childResult.cause());
                    return;
                }
                List<String> result = childResult.result();
                LOGGER.debugOp("Setting initial children {}", result);
                synchronized (this) {
                    this.children = result;
                }
                // Start watching existing children for config and partition changes
                for (String child : result) {
                    tcw.addChild(child);
                    tw.addChild(child);
                }
                this.state = 1;
            });
            return Future.succeededFuture();
        });
    }

    /**
     * Handler which runs on ZkClient's single event handling thread.
     */
    private class ChildrenWatchHandler implements Handler<AsyncResult<List<String>>> {

        private final Zk zk;
        private int watchCount = 0;

        protected ChildrenWatchHandler(Zk zk) {
            this.zk = zk;
        }

        /** Handles the event that has happened */
        @Override
        public void handle(AsyncResult<List<String>> childResult) {
            if (state == 2) {
                zk.unwatchChildren(TOPICS_ZNODE);
                return;
            }
            if (childResult.failed()) {
                LOGGER.errorOp("Error on znode {} children", TOPICS_ZNODE, childResult.cause());
                return;
            }
            ++watchCount;
            List<String> result = childResult.result();
            Set<String> deleted;
            Set<String> created;
            synchronized (ZkTopicsWatcher.this) {
                LOGGER.debugOp("{}: znode {} now has children {}, previous children {}", watchCount, TOPICS_ZNODE, result, ZkTopicsWatcher.this.children);
                List<String> oldChildren = ZkTopicsWatcher.this.children;
                if (oldChildren == null) {
                    return;
                }
                deleted = new HashSet<>(oldChildren);
                deleted.removeAll(result);
                created = new HashSet<>(result);
                created.removeAll(ZkTopicsWatcher.this.children);
                ZkTopicsWatcher.this.children = result;
            }

            LOGGER.infoOp("Topics deleted from ZK for watch {}: {}", watchCount, deleted);
            if (!deleted.isEmpty()) {
                for (String topicName : deleted) {
                    tcw.removeChild(topicName);
                    tw.removeChild(topicName);
                    LogContext logContext = LogContext.zkWatch(TOPICS_ZNODE, watchCount + ":-" + topicName, topicOperator.getNamespace(), topicName);
                    topicOperator.onTopicDeleted(logContext, new TopicName(topicName)).onComplete(ar -> {
                        if (ar.succeeded()) {
                            LOGGER.debugCr(logContext.toReconciliation(), "Success responding to deletion of topic {}", topicName);
                        } else {
                            LOGGER.warnCr(logContext.toReconciliation(), "Error responding to deletion of topic {}", topicName, ar.cause());
                        }
                    });
                }
            }

            LOGGER.infoOp("Topics created in ZK for watch {}: {}", watchCount, created);
            if (!created.isEmpty()) {
                for (String topicName : created) {
                    tcw.addChild(topicName);
                    tw.addChild(topicName);
                    LogContext logContext = LogContext.zkWatch(TOPICS_ZNODE, watchCount + ":+" + topicName, topicOperator.getNamespace(), topicName);
                    topicOperator.onTopicCreated(logContext, new TopicName(topicName)).onComplete(ar -> {
                        if (ar.succeeded()) {
                            LOGGER.debugCr(logContext.toReconciliation(), "Success responding to creation of topic {}", topicName);
                        } else {
                            LOGGER.warnCr(logContext.toReconciliation(), "Error responding to creation of topic {}", topicName, ar.cause());
                        }
                    });
                }
            }
        }
    }
}
