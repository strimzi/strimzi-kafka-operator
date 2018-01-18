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
 * ZooKeeper watcher for child znodes of {@code /configs/topics},
 * calling {@link Controller#onTopicConfigChanged(TopicName, Handler)}
 * for changed children.
 */
class TopicConfigsWatcher {

    private final static Logger logger = LoggerFactory.getLogger(TopicConfigsWatcher.class);

    private static final String CONFIGS_ZNODE = "/config/topics";

    private final Controller controller;

    private final ConcurrentHashMap<String,Boolean> children = new ConcurrentHashMap<>();
    private volatile int state = 0;
    private volatile  Zk zk;

    TopicConfigsWatcher(Controller controller) {
        this.controller = controller;
    }

    public void addChild(String child) {
        logger.debug("Watching topic {} for config changes", child);
        this.children.put(child, Boolean.FALSE);
        String path = getPath(child);
        Handler<AsyncResult<byte[]>> handler = dataResult -> {
            if (dataResult.succeeded()) {
                this.children.compute(child, (k, v) -> {
                    if (Boolean.TRUE.equals(v)) {
                        logger.debug("Config change for topic {}", child);
                        controller.onTopicConfigChanged(new TopicName(child), ar2 -> {
                            logger.info("Reconciliation result due to topic config change: {}", ar2);
                        });
                    }
                    return Boolean.TRUE;
                });
            } else {
                logger.error("While getting or watching znode {}", path, dataResult.cause());
            }
        };
        zk.watchData(path, handler).getData(path, handler);
    }

    private String getPath(String child) {
        return CONFIGS_ZNODE + "/" + child;
    }

    boolean watching(String child) {
        return children.containsKey(child);
    }

    public synchronized void removeChild(String child) {
        logger.debug("Unwatching topic {} for config changes", child);
        this.children.remove(child);
        zk.unwatchData(getPath(child));
    }

    public void start(Zk zk) {
        this.zk = zk;
        this.state = 1;
    }


    public void stop() {
        this.state = 2;
    }

    public boolean started() {
        return state == 1;
    }
}
