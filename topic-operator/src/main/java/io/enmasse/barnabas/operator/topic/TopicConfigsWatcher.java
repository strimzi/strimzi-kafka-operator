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

import io.enmasse.barnabas.operator.topic.zk.Zk;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * ZooKeeper watcher for child znodes of {@code /configs/topics},
 * calling {@link Operator#onTopicConfigChanged(TopicName, Handler)}
 * for changed children.
 */
class TopicConfigsWatcher {

    private final static Logger logger = LoggerFactory.getLogger(TopicConfigsWatcher.class);

    private static final String CONFIGS_ZNODE = "/configs/topics";

    private final Operator operator;


    private Set<String> children;
    private volatile boolean stopped;

    TopicConfigsWatcher(Operator operator) {
        this.operator = operator;
    }

    public void start(Zk zk) {
        children = new HashSet<>();
        zk.children(CONFIGS_ZNODE, true, ar -> {
            if (stopped) {
                // TODO not ideal as the Zk instance will continue watching
                return;
            }
            if (ar.succeeded()) {
                for (String child : ar.result()) {
                    zk.setData(CONFIGS_ZNODE + "/" + child, true, dataResult -> {
                        if (!this.children.add(child)) {
                            operator.onTopicConfigChanged(new TopicName(child), ar2 -> {
                            });
                        }
                    });
                }
            }
        });
        // TODO Do I need to cope with znode removal?
    }

    public void stop() {
        this.stopped = true;
    }
}
