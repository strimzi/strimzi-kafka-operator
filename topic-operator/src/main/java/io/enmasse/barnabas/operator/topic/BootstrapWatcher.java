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
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Session watcher for ZooKeeper, sets up the {@link TopicsWatcher} when a session is established. */
class BootstrapWatcher implements org.apache.zookeeper.Watcher {

    private final static Logger logger = LoggerFactory.getLogger(BootstrapWatcher.class);

    private final Operator operator;
    ZooKeeper zk0;
    private final String zookeeperConnect;

    public BootstrapWatcher(Operator operator, String zookeeperConnect){
        this.operator = operator;
        this.zookeeperConnect = zookeeperConnect;
        connect();
    }

    private void connect() {
        try {
            zk0 = new ZooKeeper(zookeeperConnect, 6000, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info("{} received {}", this, watchedEvent);
        Event.KeeperState state = watchedEvent.getState();
        if (state == Event.KeeperState.SyncConnected
                || state == Event.KeeperState.ConnectedReadOnly) {
            logger.info("{} setting topic watcher", this);
            // TODO we need watches on topic config changes and partition changes too
            new TopicsWatcher(operator, zk0).setWatch();
        } else if (state == Event.KeeperState.Disconnected) {
            connect();
        } else {
            logger.error("Not connected!");
        }
    }
}
