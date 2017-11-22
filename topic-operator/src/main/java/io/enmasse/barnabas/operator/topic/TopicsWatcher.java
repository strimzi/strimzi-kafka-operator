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

package io.enmasse.barnabas.operator.topic;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ZooKeeper watcher for child znodes of {@code /brokers/topics},
 * calling {@link Operator#onTopicCreated(TopicName, io.vertx.core.Handler)} for new children and
 * {@link Operator#onTopicDeleted(TopicName, io.vertx.core.Handler)} for deleted children.
 */
class TopicsWatcher implements Watcher {

    private final static Logger logger = LoggerFactory.getLogger(TopicsWatcher.class);

    private static final String TOPICS_ZNODE = "/brokers/topics";

    private final Operator operator;

    private final ZooKeeper zookeeper;

    private List<String> children;

    private volatile boolean shutdown = false;

    TopicsWatcher(Operator operator, ZooKeeper zookeeper) {
        this.operator = operator;
        this.zookeeper = zookeeper;
    }

    public void startShutdown() {
        shutdown = true;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (shutdown) {
            return;
        }
        logger.info("{} received {}", this, watchedEvent);
        setWatch();
    }

    void setWatch() {
        try {
            List<String> result = zookeeper.getChildren(TOPICS_ZNODE, this);

            logger.info("znode {} has children {}", TOPICS_ZNODE, result);
            if (this.children == null) {
                this.children = result;
            } else {
                logger.info("Current children {}", this.children);
                Set<String> deleted = new HashSet(this.children);
                deleted.removeAll(result);
                logger.info("Deleted topics: {}", deleted);
                for (String topicName : deleted) {
                    operator.onTopicDeleted(new TopicName(topicName), ar -> {});
                }
                Set<String> created = new HashSet(result);
                created.removeAll(this.children);
                logger.info("Created topics: {}", created);
                for (String topicName : created) {
                    operator.onTopicCreated(new TopicName(topicName), ar -> {});
                }
                logger.info("Setting current children {}", result);
                this.children = result;
            }
        } catch (Exception e1) {
            logger.error("Error setting/resetting/processing watch on {}", TOPICS_ZNODE, e1);
        }
    }
}
