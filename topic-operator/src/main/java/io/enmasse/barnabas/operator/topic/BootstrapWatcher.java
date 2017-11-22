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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Supplier;
import java.util.concurrent.CompletableFuture;

/** Session watcher for ZooKeeper, sets up the {@link TopicsWatcher} when a session is established. */
class BootstrapWatcher {

    private final static Logger logger = LoggerFactory.getLogger(BootstrapWatcher.class);

    private final Vertx vertx;
    private ZooKeeper zk0;
    private final String zookeeperConnect;
    private Handler<AsyncResult<ZooKeeper>> connectionHandler;
    private Handler<AsyncResult<Void>> disconnectionHandler;
    private volatile boolean shutdown = false;

    public BootstrapWatcher(Vertx vertx, String zookeeperConnect,
                            Handler<AsyncResult<ZooKeeper>> connectionHandler,
                            Handler<AsyncResult<Void>> disconnectionHandler){
        this.vertx = vertx;
        this.zookeeperConnect = zookeeperConnect;
        this.connectionHandler = connectionHandler;
        this.disconnectionHandler = disconnectionHandler;
        connect();
    }

    private void connect() {
        if (shutdown) {
            return;
        }
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                logger.info("{} received {}", this, watchedEvent);
                Event.KeeperState state = watchedEvent.getState();
                if (state == Event.KeeperState.SyncConnected
                        || state == Event.KeeperState.ConnectedReadOnly) {
                    logger.info("{} invoking connectionHandler {}", this, connectionHandler);
                    // TODO we need watches on partition changes too
                    vertx.runOnContext(ar -> connectionHandler.handle(Future.succeededFuture(zk0)));
                } else if (state == Event.KeeperState.Expired ||
                        state == Event.KeeperState.Disconnected) {
                    logger.info("{} invoking disconnectionHandler {}", this, disconnectionHandler);
                    vertx.runOnContext(ar -> disconnectionHandler.handle(Future.succeededFuture()));
                    connect();
                } else {
                    logger.error("Not connected! In state {}", state);
                }
            }
        };
        try {
            logger.info("Openning ZK connection to", zookeeperConnect);
            zk0 = new ZooKeeper(zookeeperConnect, 6000, watcher);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void close() {
        shutdown = true;
        try {
            zk0.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
