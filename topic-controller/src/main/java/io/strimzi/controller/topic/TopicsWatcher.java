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

    private final static Logger logger = LoggerFactory.getLogger(TopicsWatcher.class);

    private static final String TOPICS_ZNODE = "/brokers/topics";

    private final Controller controller;
    private final TopicConfigsWatcher tcw;

    private List<String> children;

    private volatile int state = 0;

    TopicsWatcher(Controller controller, TopicConfigsWatcher tcw) {
        this.controller = controller;
        this.tcw = tcw;
    }

    void stop() {
        this.state = 2;
    }

    boolean stopped() {
        return this.state == 3;
    }

    boolean started() {
        return this.state == 1;
    }

    void start(Zk zk) {
        children = null;
        tcw.start(zk);
        zk.watchChildren(TOPICS_ZNODE, childResult -> {
            if (state == 2) {
                zk.unwatchChildren(TOPICS_ZNODE);
                return;
            }
            if (childResult.failed()) {
                throw new RuntimeException(childResult.cause());
            }
            List<String> result = childResult.result();
            logger.debug("znode {} now has children {}, previous children {}", TOPICS_ZNODE, result, this.children);
            Set<String> deleted = new HashSet(this.children);
            deleted.removeAll(result);
            Set<String> created = new HashSet(result);
            created.removeAll(this.children);
            this.children = result;

            if (!deleted.isEmpty()) {
                logger.info("Deleted topics: {}", deleted);
                for (String topicName : deleted) {
                    tcw.removeChild(topicName);
                    controller.onTopicDeleted(new TopicName(topicName), ar -> {
                        if (ar.succeeded()) {
                            logger.debug("Success responding to deletion of topic {}", topicName);
                        } else {
                            logger.warn("Error responding to deletion of topic {}", topicName, ar.cause());
                        }
                    });
                }
            }

            if (!created.isEmpty()) {
                logger.info("Created topics: {}", created);
                for (String topicName : created) {
                    tcw.addChild(topicName);
                    controller.onTopicCreated(new TopicName(topicName), ar -> {
                        if (ar.succeeded()) {
                            logger.debug("Success responding to creation of topic {}", topicName);
                        } else {
                            logger.warn("Error responding to creation of topic {}", topicName, ar.cause());
                        }
                    });
                }
            }

        }).children(TOPICS_ZNODE, childResult -> {
            if (childResult.failed()) {
                throw new RuntimeException(childResult.cause());
            }
            List<String> result = childResult.result();
            logger.debug("Setting initial children {}", result);
            this.children = result;
            this.state = 1;
        });
    }
}
