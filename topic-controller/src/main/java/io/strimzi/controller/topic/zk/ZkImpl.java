/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of {@link Zk}
 */
public class ZkImpl implements Zk {

    private final static Logger LOGGER = LoggerFactory.getLogger(ZkImpl.class);
    public static final String PREFIX_DATA = "data:";
    public static final String PREFIX_CHILDREN = "children:";
    public static final String PREFIX_EXISTS = "exists:";
    private final boolean readOnly;

    private final String zkConnectionString;
    private final int sessionTimeout;
    private final Vertx vertx;
    private final ZooKeeper zk;

    // Only accessed on the vertx context.
    private final ConcurrentHashMap<String, Handler<? extends AsyncResult<?>>> watches = new ConcurrentHashMap<>();

    // TODO We need to reset the watches on reconnection.
    // TODO We need to retry methods which fail due to connection loss, up to some limit/time
    // We should probably try to avoid stampede though, so random exponential backoff

    public ZkImpl(Vertx vertx, String zkConnectionString, int sessionTimeout, boolean readOnly) {
        this.vertx = vertx;
        this.zkConnectionString = zkConnectionString;
        this.sessionTimeout = sessionTimeout;
        this.readOnly = readOnly;
        CompletableFuture f = new CompletableFuture();
        try {
            zk = new ZooKeeper(zkConnectionString, sessionTimeout, watchedEvent -> {
                // See https://wiki.apache.org/hadoop/ZooKeeper/FAQ
                // for state transitions
                Watcher.Event.KeeperState state = watchedEvent.getState();
                LOGGER.debug("In state {}", state);
                final Future<Zk> future;
                final Handler<AsyncResult<Zk>> handler;
                switch (state) {
                    case AuthFailed:
                        f.completeExceptionally(new RuntimeException("Zookeeper authentication failed"));
                    case SaslAuthenticated:
                        // TODO record that we're auth, so methods can reject ACLs with "auth" scheme?
                        break;
                    case ConnectedReadOnly:
                        if (!readOnly) {
                            // This should never happen
                            throw new RuntimeException("Connected readonly");
                        }
                        /* fall through */
                    case SyncConnected:
                        LOGGER.debug("Connected, session id {}", zk().getSessionId());
                        f.complete(null);
                        break;
                    case Expired:
                        // We've just been reconnected to the emsemble, and our session has expired while
                        // we were disconnected
                        f.complete(null);
                        break;
                    case Disconnected:
                        // We've just been disconnected from the emsemble. The ZooKeeper implementation
                        // should reconnect us soon.
                        break;
                    default:
                        // According to the KeeperState doc
                        // the remaining states should be impossible
                        throw new IllegalStateException("Unexpected state: " + state.toString() + "");
                }
            },
            readOnly);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            f.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    private ZooKeeper zk() {
        return zk;
    }


    /**
     * Map the given rc result code to a KeeperException, then run the given handler on the vertx context.
     */
    private <T> void invokeOnContext(Handler<AsyncResult<T>> handler, String path, int rc, T result) {
        Future<T> future = mapResult(path, rc, result);
        vertx.runOnContext(ignored -> handler.handle(future));
    }

    private <T> Future<T> mapResult(String path, int rc, T result) {
        KeeperException.Code code = KeeperException.Code.get(rc);
        Future<T> future;
        switch (code) {
            case OK:
                future = Future.succeededFuture(result);
                break;
            default:
                future = Future.failedFuture(KeeperException.create(code, "for path: " + path));
        }
        return future;
    }

    @Override
    public Zk create(String path, byte[] data, List<ACL> acls, CreateMode createMode, Handler<AsyncResult<Void>> handler) {
        ZooKeeper zookeeper;
        synchronized (this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
            return this;
        }
        zookeeper.create(path, data == null ? new byte[0] : data, acls, createMode,
            (rc, path2, ctx, name) -> invokeOnContext(handler, path, rc, null), null);
        return this;
    }


    @Override
    public Zk setData(String path, byte[] data, int version, Handler<AsyncResult<Void>> handler) {
        ZooKeeper zookeeper;
        synchronized (this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
            return this;
        }
        zookeeper.setData(path, data, version,
            (int rc, String path2, Object ctx, Stat stat) -> invokeOnContext(handler, path, rc, null),
                null);
        return this;
    }

    public Zk disconnect() throws InterruptedException {
        zk.close();
        return this;
    }

    @Override
    public Zk getData(String path, Handler<AsyncResult<byte[]>> handler) {
        ZooKeeper zookeeper;
        synchronized (this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
            return this;
        }
        final AsyncCallback.DataCallback callback = (rc, path2, ctx, data, stat) -> {
            Watcher.Event.EventType eventType = (Watcher.Event.EventType) ctx;
            if (eventType == null // first time
                    || eventType == Watcher.Event.EventType.NodeDataChanged) {
                Future<byte[]> future = mapResult(path2, rc, data);
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
        return (Handler<AsyncResult<byte[]>>) watches.get(PREFIX_DATA + path);
    }

    private Handler<AsyncResult<List<String>>> getChildrenWatchHandler(String path) {
        return (Handler<AsyncResult<List<String>>>) watches.get(PREFIX_CHILDREN + path);
    }

    private Handler<AsyncResult<Stat>> getExistsWatchHandler(String path) {
        return (Handler<AsyncResult<Stat>>) watches.get(PREFIX_EXISTS + path);
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
        synchronized (this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
            return this;
        }
        Object ctx = null;
        zookeeper.delete(path, version, (rc, path1, ctx1) -> invokeOnContext(handler, path, rc, null), ctx);
        return this;
    }

    @Override
    public Zk exists(String path, Handler<AsyncResult<Stat>> handler) {
        ZooKeeper zookeeper;
        synchronized (this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
            return this;
        }
        final AsyncCallback.StatCallback callback = (rc, path1, ctx1, stat) -> {
            Watcher.Event.EventType eventType = (Watcher.Event.EventType) ctx1;
            if (eventType == null // first time
                    || eventType == Watcher.Event.EventType.NodeCreated
                    || eventType == Watcher.Event.EventType.NodeDeleted
                    || KeeperException.Code.get(rc) != KeeperException.Code.OK) {
                Future<Stat> future = mapResult(path1, rc, stat);
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
        synchronized (this) {
            zookeeper = zk;
        }
        if (zookeeper == null) {
            handler.handle(Future.failedFuture(new IllegalStateException("Not connected")));
            return this;
        }
        final AsyncCallback.Children2Callback callback = (rc, path2, ctx, children, stat) -> {
            Watcher.Event.EventType eventType = (Watcher.Event.EventType) ctx;
            KeeperException.Code code = KeeperException.Code.get(rc);
            LOGGER.debug("{}: {} {}", path2, eventType, code);
            if (eventType == null // first time
                    || eventType == Watcher.Event.EventType.NodeChildrenChanged
                    || code != KeeperException.Code.OK) {
                Future<List<String>> future = mapResult(path2, rc, children);
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
