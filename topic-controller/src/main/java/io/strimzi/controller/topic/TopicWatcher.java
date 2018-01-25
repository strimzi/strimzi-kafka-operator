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
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * ZooKeeper watcher for child znodes of {@code /brokers/topics},
 */
public class TopicWatcher {

    private static final Logger log = LoggerFactory.getLogger(TopicWatcher.class);

    private static final String TOPICS_ZNODE = "/brokers/topics";

    private final Controller controller;

    private final ConcurrentHashMap<String,Boolean> children = new ConcurrentHashMap<>();
    private volatile int state = 0;
    private volatile Zk zk;

    /**
     * Contructor
     *
     * @param controller    Controller instance to notify
     */
    TopicWatcher(Controller controller) {
        this.controller = controller;
    }

    /**
     * Add a child to watch under the topics znode
     *
     * @param child child to watch
     */
    public void addChild(String child) {
        log.debug("Watching topic {} for partitions changes", child);
        this.children.put(child, false);
        String path = getPath(child);
        Handler<AsyncResult<byte[]>> handler = dataResult -> {
            if (dataResult.succeeded()) {
                this.children.compute(child, (k, v) -> {
                    if (v) {
                        log.debug("Partitions change for topic {}", child);
                        controller.onTopicPartitionsChanged(new TopicName(child), ar -> {
                                log.info("Reconciliation result due to topic partitions change: {}", ar);
                        });
                    }
                    return true;
                });
            } else {
                log.error("While getting or watching znode {}", path, dataResult.cause());
            }
        };
        zk.watchData(path, handler).getData(path, handler);
    }

    /**
     * Return the path of the watched topic
     *
     * @param child child to watch
     * @return  full path of the znode
     */
    private String getPath(String child) {
        return TOPICS_ZNODE + "/" + child;
    }

    /**
     * Check if the provided child is currently watched
     *
     * @param child child to check
     * @return  If the passed child is currently watched
     */
    boolean watching(String child) {
        return children.containsKey(child);
    }

    public synchronized void removeChild(String child) {
        log.debug("Unwatching topic {} for partitions changes", child);
        this.children.remove(child);
        zk.unwatchData(getPath(child));
    }

    /**
     * Start the watcher
     *
     * @param zk    Zookeeper client instance
     */
    public void start(Zk zk) {
        this.zk = zk;
        this.state = 1;
    }

    /**
     * Stop the watcher
     */
    public void stop() {
        this.state = 2;
    }

    /**
     * @return  if the watcher is already started
     */
    public boolean started() {
        return state == 1;
    }


}
