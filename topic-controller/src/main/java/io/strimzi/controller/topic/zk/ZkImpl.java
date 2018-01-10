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

package io.strimzi.controller.topic.zk;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptySet;

/**
 * Implementation of {@link Zk}
 */
public class ZkImpl implements Zk {

    private final static Logger logger = LoggerFactory.getLogger(ZkImpl.class);
    public static final String PREFIX_DATA = "data:";
    public static final String PREFIX_CHILDREN = "children:";
    public static final String PREFIX_EXISTS = "exists:";
    private Handler<AsyncResult<ZooKeeper>> temporaryConnectionHandler;


    private static <T> Map<String, Set<Handler<AsyncResult<T>>>>
    removeWatch(Map<String, Set<Handler<AsyncResult<T>>>> watches, String path, Handler<AsyncResult<T>> handler) {
        if (watches != null) {
            Set<Handler<AsyncResult<T>>> handlers = watches.get(path);
            if (handlers != null) {
                handlers.remove(handler);
                if (handlers.isEmpty()) {
                    watches.remove(path);
                }
            }
            if (watches.isEmpty()) {
                watches = null;
            }
        }
        return watches;
    }

    private final String zkConnectionString;
    private final int sessionTimeout;
    private final Vertx vertx;
    private ZooKeeper zk;
    private Handler<AsyncResult<Zk>> disconnectionHandler;

    // Only accessed on the vertx context.
    private final ConcurrentHashMap<String, Handler<? extends AsyncResult<?>>> watches = new ConcurrentHashMap<>();

    public ZkImpl(Vertx vertx, String zkConnectionString, int sessionTimeout) {
        this.vertx = vertx;
        this.zkConnectionString = zkConnectionString;
        this.sessionTimeout = sessionTimeout;
    }

    @Override
    public synchronized Zk connect(Handler<AsyncResult<Zk>> connectionHandler) {
        if (zk == null) {
            try {
                zk = new ZooKeeper(zkConnectionString, sessionTimeout, watchedEvent -> {
                    Watcher.Event.KeeperState state = watchedEvent.getState();
                    logger.debug("In state {}", state);
                    final Future<Zk> future;
                    final Handler<AsyncResult<Zk>> handler;
                    switch (state) {
                        case AuthFailed:
                            future = Future.failedFuture(new IllegalStateException("ZooKeeper authentication failed"));
                            handler = connectionHandler;
                            break;
                        case SaslAuthenticated:
                            // TODO add callback for SASL handshake
                            future = Future.failedFuture(new RuntimeException("TODO add callback for SASL handshake"));
                            handler = connectionHandler;
                            break;
                        case SyncConnected:
                        case ConnectedReadOnly:
                            future = Future.succeededFuture(this);
                            handler = connectionHandler;
                            // TODO fix this: get rid of this temporary handler: The zkTopicStore should use a Zk not a ZooKeeper
                            if (temporaryConnectionHandler != null) {
                                temporaryConnectionHandler.handle(Future.succeededFuture(zk));
                            }
                            break;
                        case Expired:
                        case Disconnected:
                            // To get to these states we must have been connected
                            zk = null;
                            future = Future.succeededFuture(this);
                            handler = disconnectionHandler;
                            break;
                        default:
                            // According to the KeeperState doc
                            // the remaining states should be impossible
                            future = Future.failedFuture(new IllegalStateException("Unexpected state: " + state.toString() + ""));
                            handler = connectionHandler;
                    }
                    if (future != null && handler != null) {
                        vertx.runOnContext(ar -> handler.handle(future));
                    }
                });
            } catch (IOException e) {
                connectionHandler.handle(Future.failedFuture(e));
            }
        } else {
            connectionHandler.handle(Future.failedFuture(new IllegalStateException("Already connected")));
        }
        return this;
    }

    @Override
    public Zk temporaryConnectionHandler(Handler<AsyncResult<ZooKeeper>> handler) {
        this.temporaryConnectionHandler = handler;
        return this;
    }

    @Override
    public Zk disconnectionHandler(Handler<AsyncResult<Zk>> handler) {
        this.disconnectionHandler = handler;
        return this;
    }

    /**
     * Map the given rc result code to a KeeperException, then run the given handler on the vertx context.
     */
    private <T> void invokeOnContext(Handler<AsyncResult<T>> handler, int rc, T result) {
        KeeperException.Code code = KeeperException.Code.get(rc);
        Future<T> future;
        switch (code) {
            case OK:
                future = Future.succeededFuture(result);
                break;
            default:
                future = Future.failedFuture(KeeperException.create(code));
        }
        vertx.runOnContext(ignored -> handler.handle(future));
    }

    private static <T> Future<T> mapResult(int rc, T result) {
        KeeperException.Code code = KeeperException.Code.get(rc);
        Future<T> future;
        switch (code) {
            case OK:
                future = Future.succeededFuture(result);
                break;
            default:
                future = Future.failedFuture(KeeperException.create(code));
        }
        return future;
    }

    @Override
    public Zk create(String path, byte[] data, List<ACL> acls, CreateMode createMode, Handler<AsyncResult<Void>> handler) {
        ZooKeeper zookeeper;
        synchronized(this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
        }
        zookeeper.create(path, data == null ? new byte[0] : data, acls, createMode,
        (rc, path2, ctx, name) -> invokeOnContext(handler, rc, null), null);
        return this;
    }


    @Override
    public Zk setData(String path, byte[] data, int version, Handler<AsyncResult<Void>> handler) {
        ZooKeeper zookeeper;
        synchronized(this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
        }
        zookeeper.setData(path, data, version,
                (int rc, String path2, Object ctx, Stat stat) -> invokeOnContext(handler, rc, null),
                null);
        return this;
    }

    @Override
    public Zk disconnect(Handler<AsyncResult<Void>> handler) {

        vertx.<Void>executeBlocking((f) -> {
            logger.error("Disconnecting");
            if (zk == null) {
                f.fail(new IllegalStateException("Not connected"));
            } else {
                try {
                    zk.close();
                    logger.error("Disconnected");
                    f.complete();
                } catch (InterruptedException e) {
                    f.fail(e);
                }
            }
        }, ar-> {
            zk = null;
            if (this.disconnectionHandler != null) {
                vertx.runOnContext(
                        ignored -> this.disconnectionHandler.handle(ar.map(x->null)));
            }
            handler.handle(ar);
        });
        return this;
    }

    @Override
    public Zk getData(String path, Handler<AsyncResult<byte[]>> handler) {
        ZooKeeper zookeeper;
        synchronized(this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
        }
        final AsyncCallback.DataCallback callback = (rc, path2, ctx, data, stat) -> {
            Watcher.Event.EventType eventType = (Watcher.Event.EventType)ctx;
            if (eventType == null // first time
                    || eventType == Watcher.Event.EventType.NodeDataChanged
                    || KeeperException.Code.get(rc) != KeeperException.Code.OK) {
                Future<byte[]> future = mapResult(rc, data);
                vertx.runOnContext(ignored -> {
                    final Handler<AsyncResult<byte[]>> watch = getDataWatchHandler(path);
                    if (eventType != null && watch != null) {
                        // Only call the handlers if callback fired due to watch
                        watch.handle(future);
                    }
                    if (eventType == null && handler != null) {
                        handler.handle(future);
                    }
                });
            }
        };
        final Watcher watcher;
        if (getDataWatchHandler(path) != null) {
            watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (getDataWatchHandler(path) != null) {
                        // Reset the watch if there still is a handler
                        zookeeper.getData(path, this,
                                callback, event.getType());
                    }
                }
            };
        } else {
            watcher = null;
        }
        zookeeper.getData(path, watcher, callback, null);
        return this;
    }

    private Handler<AsyncResult<byte[]>> getDataWatchHandler(String path) {
        return (Handler<AsyncResult<byte[]>>)watches.get(PREFIX_DATA + path);
    }

    private Handler<AsyncResult<List<String>>> getChildrenWatchHandler(String path) {
        return (Handler<AsyncResult<List<String>>>)watches.get(PREFIX_CHILDREN + path);
    }

    private Handler<AsyncResult<Stat>> getExistsWatchHandler(String path) {
        return (Handler<AsyncResult<Stat>>)watches.get(PREFIX_EXISTS + path);
    }

    @Override
    public Zk watchData(String path, Handler<AsyncResult<byte[]>> watcher) {
        watches.put(PREFIX_DATA + path, watcher);
        return this;
    }

    @Override
    public Zk unwatchData(String path) {
        watches.remove(PREFIX_DATA + path);
        return this;
    }

    @Override
    public Zk delete(String path, int version, Handler<AsyncResult<Void>> handler) {
        ZooKeeper zookeeper;
        synchronized(this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
        }
        Object ctx = null;
        zookeeper.delete(path, version, (rc, path1, ctx1) -> invokeOnContext(handler, rc, null), ctx);
        return this;
    }

    @Override
    public Zk exists(String path, Handler<AsyncResult<Stat>> handler) {
        ZooKeeper zookeeper;
        synchronized(this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
        }
        final AsyncCallback.StatCallback callback = (rc, path1, ctx1, stat) -> {
            Watcher.Event.EventType eventType = (Watcher.Event.EventType) ctx1;
            if (eventType == null // first time
                    || eventType == Watcher.Event.EventType.NodeCreated
                    || eventType == Watcher.Event.EventType.NodeDeleted
                    || KeeperException.Code.get(rc) != KeeperException.Code.OK) {
                Future<Stat> future = mapResult(rc, stat);
                vertx.runOnContext(ignored -> {
                    final Handler<AsyncResult<Stat>> watch = getExistsWatchHandler(path);
                    if (eventType != null && watch != null) {
                        // Only call the handlers if callback fired due to watch
                        watch.handle(future);
                    }
                    if (eventType == null && handler != null) {
                        handler.handle(future);
                    }
                });
            }
        };
        final Watcher watcher;
        if (getExistsWatchHandler(path) != null) {
            watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (getExistsWatchHandler(path) != null) {
                        // Reset the watch if there still is a handler
                        zookeeper.exists(path, this,
                                callback, event.getType());
                    }
                }
            };
        } else {
            watcher = null;
        }
        zookeeper.exists(path, watcher, callback, null);
        return this;
    }

    @Override
    public Zk watchExists(String path, Handler<AsyncResult<Stat>> watcher) {
        watches.put(PREFIX_EXISTS + path, watcher);
        return this;
    }

    @Override
    public Zk unwatchExists(String path) {
        watches.remove(PREFIX_EXISTS + path);
        return this;
    }

    @Override
    public Zk children(String path, Handler<AsyncResult<List<String>>> handler) {
        ZooKeeper zookeeper;
        synchronized(this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
            return this;
        }
        final AsyncCallback.Children2Callback callback = (rc, path2, ctx, children, stat) -> {
            Watcher.Event.EventType eventType = (Watcher.Event.EventType)ctx;
            if (eventType == null // first time
                    || eventType == Watcher.Event.EventType.NodeChildrenChanged
                    || KeeperException.Code.get(rc) != KeeperException.Code.OK) {
                Future<List<String>> future = mapResult(rc, children);
                vertx.runOnContext(ignored -> {
                    final Handler<AsyncResult<List<String>>> watch = getChildrenWatchHandler(path);
                    if (eventType != null && watch != null) {
                        // Only call the handlers if callback fired due to watch
                        watch.handle(future);
                    }
                    if (eventType == null && handler != null) {
                        handler.handle(future);
                    }
                });
            }
        };
        final Watcher watcher;
        if (getChildrenWatchHandler(path) != null) {
            watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (getChildrenWatchHandler(path) != null) {
                        // Reset the watch if there still is a handler
                        zookeeper.getChildren(path, this,
                                callback, event.getType());
                    }
                }
            };
        } else {
            watcher = null;
        }
        zookeeper.getChildren(path, watcher, callback, null);
        return this;
    }

    @Override
    public Zk watchChildren(String path, Handler<AsyncResult<List<String>>> watcher) {
        watches.put(PREFIX_CHILDREN + path, watcher);
        return this;
    }

    @Override
    public Zk unwatchChildren(String path) {
        watches.remove(PREFIX_CHILDREN + path);
        return this;
    }



}
