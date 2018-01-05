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
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * ZooKeeper watcher for child znodes of {@code /configs/topics},
 * calling {@link Controller#onTopicConfigChanged(TopicName, Handler)}
 * for changed children.
 */
class TopicConfigsWatcher {

    private final static Logger logger = LoggerFactory.getLogger(TopicConfigsWatcher.class);

    private static final String CONFIGS_ZNODE = "/configs/topics";

    private final Controller controller;

    private Set<String> children;
    private volatile int state = 0;

    TopicConfigsWatcher(Controller controller) {
        this.controller = controller;
    }

    public void start(Zk zk) {
        children = new HashSet<>();
        zk.children(CONFIGS_ZNODE, true, ar -> {
            if (state == 2) {
                state = 3;
                return;
            }
            if (ar.succeeded()) {
                for (String child : ar.result()) {
                    zk.setData(CONFIGS_ZNODE + "/" + child, true, dataResult -> {
                        if (!this.children.add(child)) {
                            controller.onTopicConfigChanged(new TopicName(child), ar2 -> {
                            });
                        }
                    });
                }
            }
        });
        // TODO Do I need to cope with znode removal?
        this.state = 1;
    }

    public void stop() {
        this.state = 2;
    }

    public boolean started() {
        return state == 1;
    }
}
