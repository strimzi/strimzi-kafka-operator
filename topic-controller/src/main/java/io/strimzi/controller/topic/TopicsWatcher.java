/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.strimzi.controller.topic.zk.Zk;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ZooKeeper watcher for child znodes of {@code /brokers/topics},
 * calling {@link Controller#onTopicCreated(TopicName, io.vertx.core.Handler)} for new children and
 * {@link Controller#onTopicDeleted(TopicName, io.vertx.core.Handler)} for deleted children.
 */
class TopicsWatcher {

    private final static Logger LOGGER = LoggerFactory.getLogger(TopicsWatcher.class);

    private static final String TOPICS_ZNODE = "/brokers/topics";

    private final Controller controller;
    private final TopicConfigsWatcher tcw;
    private final TopicWatcher tw;

    private List<String> children;

    private volatile int state = 0;

    /**
     * Constructor
     *
     * @param controller    Controller instance
     * @param tcw   watcher for the topics config changes
     * @param tw    watcher for the topics partitions changes
     */
    TopicsWatcher(Controller controller, TopicConfigsWatcher tcw, TopicWatcher tw) {
        this.controller = controller;
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
        children = null;
        tcw.start(zk);
        tw.start(zk);
        zk.watchChildren(TOPICS_ZNODE, childResult -> {
            if (state == 2) {
                zk.unwatchChildren(TOPICS_ZNODE);
                return;
            }
            if (childResult.failed()) {
                throw new RuntimeException(childResult.cause());
            }
            List<String> result = childResult.result();
            LOGGER.debug("znode {} now has children {}, previous children {}", TOPICS_ZNODE, result, this.children);
            Set<String> deleted = new HashSet(this.children);
            deleted.removeAll(result);
            Set<String> created = new HashSet(result);
            created.removeAll(this.children);
            this.children = result;

            if (!deleted.isEmpty()) {
                LOGGER.info("Deleted topics: {}", deleted);
                for (String topicName : deleted) {
                    tcw.removeChild(topicName);
                    tw.removeChild(topicName);
                    controller.onTopicDeleted(new TopicName(topicName), ar -> {
                        if (ar.succeeded()) {
                            LOGGER.debug("Success responding to deletion of topic {}", topicName);
                        } else {
                            LOGGER.warn("Error responding to deletion of topic {}", topicName, ar.cause());
                        }
                    });
                }
            }

            if (!created.isEmpty()) {
                LOGGER.info("Created topics: {}", created);
                for (String topicName : created) {
                    tcw.addChild(topicName);
                    tw.addChild(topicName);
                    controller.onTopicCreated(new TopicName(topicName), ar -> {
                        if (ar.succeeded()) {
                            LOGGER.debug("Success responding to creation of topic {}", topicName);
                        } else {
                            LOGGER.warn("Error responding to creation of topic {}", topicName, ar.cause());
                        }
                    });
                }
            }

        }).children(TOPICS_ZNODE, childResult -> {
            if (childResult.failed()) {
                throw new RuntimeException(childResult.cause());
            }
            List<String> result = childResult.result();
            LOGGER.debug("Setting initial children {}", result);
            this.children = result;
            this.state = 1;
        });
    }
}
