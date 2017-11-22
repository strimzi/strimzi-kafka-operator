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
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public interface Zk {

    Zk connect(Handler<AsyncResult<Zk>> handler);

    Zk disconnectionHandler(Handler<AsyncResult<Zk>> handler);

    /**
     * Asynchronously create the node with the given path.
     * @param path
     * @param data
     * @param handler
     * @return
     */
    Zk create(String path, byte[] data, Handler<AsyncResult<Void>> handler);

    /**
     * Register a handler to be called with the children of the given path,
     * and, if watch is true, whenever the children subsequently change.
     *
     * The handler is passed a list, whose order is undefined,
     * of the paths of the children relative to
     * the given path. For example, if the path is {@code /foo} and the znode
     * {@code /foo/bar} is added then the handler will be called with a
     * list containing {@code bar}, in addition to the other child
     * znodes of {@code /foo}.
     */
    Zk children(String path, boolean watch, Handler<AsyncResult<List<String>>> handler);

    /**
     * Register a handler to be called with the data of the given path,
     * and, if watch is true, whenever that data subsequently changes.
     */
    Zk data(String path, boolean watch, Handler<AsyncResult<byte[]>> handler);

    Zk setData(String path, byte[] data, Handler<AsyncResult<Void>> handler);
}

class ZkImpl implements Zk {

    private final static Logger logger = LoggerFactory.getLogger(ZkImpl.class);

    private final String zkConnectionString;
    private final int sessionTimeout;
    private final Vertx vertx;
    private final ACL acl;
    private ZooKeeper zk;
    private Handler<AsyncResult<Zk>> disconnectionHandler;

    public ZkImpl(Vertx vertx, String zkConnectionString, int sessionTimeout) {
        this.vertx = vertx;
        this.zkConnectionString = zkConnectionString;
        this.sessionTimeout = sessionTimeout;
        acl = new ACL();
        String scheme = "world";
        String id = "anyone";
        int perm = ZooDefs.Perms.READ | ZooDefs.Perms.WRITE |ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE;
        acl.setId(new Id(scheme, id));
        acl.setPerms(perm);
    }

    @Override
    public synchronized Zk connect(Handler<AsyncResult<Zk>> connectionHandler) {
        if (zk == null) {
            try {
                zk = new ZooKeeper(zkConnectionString, sessionTimeout, watchedEvent -> {
                    Watcher.Event.KeeperState state = watchedEvent.getState();
                    logger.error("In state {}", state);
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
                            break;
                        case Expired:
                        case Disconnected:
                            // To get to these states we must have been connected
                            zk = null;
                            future = Future.succeededFuture();
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
    public Zk disconnectionHandler(Handler<AsyncResult<Zk>> handler) {
        this.disconnectionHandler = handler;
        return this;
    }

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

    @Override
    public Zk create(String path, byte[] data, Handler<AsyncResult<Void>> handler) {
        ZooKeeper zookeeper;
        synchronized(this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
        }
        zookeeper.create(path, data == null ? new byte[0] : data, Collections.singletonList(acl), CreateMode.PERSISTENT,
        (rc, path2, ctx, name) -> invokeOnContext(handler, rc, null), null);
        return this;
    }


    @Override
    public Zk setData(String path, byte[] data, Handler<AsyncResult<Void>> handler) {
        ZooKeeper zookeeper;
        synchronized(this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
        }
        zookeeper.setData(path, data, -1,
                (int rc, String path2, Object ctx, Stat stat) -> invokeOnContext(handler, rc, null),
                null);
        return this;
    }


    public void disconnect(Handler<AsyncResult<Void>> handler) {

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

    }

    @Override
    public Zk children(String path, boolean watch, Handler<AsyncResult<List<String>>> handler) {
        ZooKeeper zookeeper;
        synchronized(this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
        }
        final AsyncCallback.Children2Callback callback = (rc, path2, ctx, children, stat) -> {
            Watcher.Event.EventType eventType = (Watcher.Event.EventType)ctx;
            if (eventType == null // first time
                || eventType == Watcher.Event.EventType.NodeChildrenChanged
                || KeeperException.Code.get(rc) != KeeperException.Code.OK) {
                // TODO are other event types even possible?
                invokeOnContext(handler, rc, children);
            }
        };
        final Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                zookeeper.getChildren(path, this,
                        callback, event.getType());
            }
        };
        zookeeper.getChildren(path, watch ? watcher : null,
                callback, null);
        return this;
    }

    @Override
    public synchronized Zk data(String path, boolean watch, Handler<AsyncResult<byte[]>> handler) {
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
                // TODO are other event types even possible?
                invokeOnContext(handler, rc, data);
            }
        };
        final Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                zookeeper.getData(path, this,
                        callback, event.getType());
            }
        };
        zookeeper.getData(path, watch ? watcher : null,
                callback, null);
        return this;
    }



}
