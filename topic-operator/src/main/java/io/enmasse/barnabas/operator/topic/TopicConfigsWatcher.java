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

import io.vertx.core.Handler;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * ZooKeeper watcher for child znodes of {@code /configs/topics},
 * calling {@link Operator#onTopicConfigChanged(TopicName, Handler)}
 * for changed children.
 */
class TopicConfigsWatcher implements Watcher {

    private final static Logger logger = LoggerFactory.getLogger(TopicConfigsWatcher.class);

    private static final String CONFIGS_ZNODE = "/configs/topics";

    private final Operator operator;

    private final ZooKeeper zookeeper;

    private volatile boolean shutdown = false;

    TopicConfigsWatcher(Operator operator, ZooKeeper zookeeper) {
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
        logger.debug("{} received {}", this, watchedEvent);
        String path = watchedEvent.getPath();
        Event.EventType type = watchedEvent.getType();

        if (type != Event.EventType.NodeDeleted) {
            logger.debug("Resetting watch on znode {}", path);
            setWatch(path, type == Event.EventType.NodeDataChanged);
        }
    }

    private void setWatch(String path, boolean dataChanged) {
        zookeeper.getData(path, this, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (dataChanged) {
                    String subpath = path.substring(CONFIGS_ZNODE.length()+1);
                    operator.onTopicConfigChanged(new TopicName(subpath), ar -> {});
                }
            }
        }, null);
    }

    void start() {
        try {
            final AsyncCallback.ChildrenCallback cb = new AsyncCallback.ChildrenCallback() {
                private Set<String> currentChildren = new HashSet<>();
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children) {
                    Set<String> newChildren = new HashSet<>(children);
                    newChildren.removeAll(currentChildren);
                    for (String child : newChildren) {
                        setWatch(child, false);
                    }
                    currentChildren = new HashSet<>(children);
                }
            };
            final Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (shutdown) {
                        return;
                    }
                    // reset the watch, so we know about future (new) children
                    zookeeper.getChildren(CONFIGS_ZNODE, this, cb, null);
                }
            };

            zookeeper.getChildren(CONFIGS_ZNODE, watcher, cb, null);

        } catch (Exception e1) {
            logger.error("Error setting watch on {}", CONFIGS_ZNODE, e1);
        }
    }
}
