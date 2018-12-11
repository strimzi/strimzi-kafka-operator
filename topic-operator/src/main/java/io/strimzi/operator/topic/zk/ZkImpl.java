/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.zk;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@link Zk}
 */
public class ZkImpl implements Zk {

    private final static Logger LOGGER = LogManager.getLogger(ZkImpl.class);
    private static final <T> Handler<AsyncResult<T>> log(String msg) {
        return ignored -> {
            LOGGER.trace("{} returned {}", msg, ignored);
        };
    }
    private final Vertx vertx;
    private final ZkClient zookeeper;

    // Only accessed on the vertx context.

    private final ConcurrentHashMap<String, IZkDataListener> dataWatches = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IZkChildListener> childWatches = new ConcurrentHashMap<>();

    public ZkImpl(Vertx vertx, String zkConnectionString, int sessionTimeout, int connectionTimeout) {
        this.vertx = vertx;
        this.zookeeper = new ZkClient(zkConnectionString, sessionTimeout, connectionTimeout, new BytesPushThroughSerializer());
    }


    @Override
    public Zk create(String path, byte[] data, List<ACL> acls, CreateMode createMode, Handler<AsyncResult<Void>> handler) {
        workerPool().executeBlocking(
            future -> {
                try {
                    zookeeper.create(path, data == null ? new byte[0] : data, acls, createMode);
                    future.complete();
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            handler);
        return this;
    }

    @Override
    public Zk setData(String path, byte[] data, int version, Handler<AsyncResult<Void>> handler) {
        workerPool().executeBlocking(
            future -> {
                try {
                    zookeeper.writeData(path, data, version);
                    future.complete();
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            handler);
        return this;
    }

    @Override
    public Zk disconnect(Handler<AsyncResult<Void>> handler) {

        workerPool().executeBlocking(
            future -> {
                try {
                    zookeeper.close();
                    future.complete();
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            handler);
        return this;
    }

    @Override
    public Zk getData(String path, Handler<AsyncResult<byte[]>> handler) {
        workerPool().executeBlocking(
            future -> {
                try {
                    future.complete(zookeeper.readData(path));
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            handler);
        return this;
    }

    static class DataWatchAdapter implements IZkDataListener {

        private final Handler<AsyncResult<byte[]>> watcher;

        public DataWatchAdapter(Handler<AsyncResult<byte[]>> watcher) {
            this.watcher = watcher;
        }

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            watcher.handle(Future.succeededFuture((byte[]) data));
        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception {

        }
    }

    @Override
    public Zk watchData(String path, Handler<AsyncResult<byte[]>> watcher) {
        workerPool().executeBlocking(
            future -> {
                try {
                    IZkDataListener listener = new DataWatchAdapter(watcher);
                    dataWatches.put(path, listener);
                    zookeeper.subscribeDataChanges(path, listener);
                    future.complete();
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            log("watchData"));
        return this;
    }

    @Override
    public Zk unwatchData(String path) {
        workerPool().executeBlocking(
            future -> {
                try {
                    IZkDataListener listener = dataWatches.remove(path);
                    if (listener != null) {
                        zookeeper.unsubscribeDataChanges(path, listener);
                    }
                    future.complete();
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            log("unwatchData"));
        return this;
    }

    @Override
    public Zk delete(String path, int version, Handler<AsyncResult<Void>> handler) {
        workerPool().executeBlocking(
            future -> {
                try {
                    if (zookeeper.delete(path, version)) {
                        future.complete();
                    } else {
                        future.fail(new ZkNoNodeException());
                    }
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            handler);
        return this;
    }

    private WorkerExecutor workerPool() {
        return vertx.createSharedWorkerExecutor(getClass().getName(), 4);
    }

    @Override
    public Zk children(String path, Handler<AsyncResult<List<String>>> handler) {
        workerPool().executeBlocking(
            future -> {
                try {
                    future.complete(zookeeper.getChildren(path));
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            handler);
        return this;

    }

    @Override
    public Zk watchChildren(String path, Handler<AsyncResult<List<String>>> watcher) {
        workerPool().executeBlocking(
            future -> {
                try {
                    IZkChildListener listener = (parentPath, currentChilds) -> watcher.handle(Future.succeededFuture(currentChilds));
                    childWatches.put(path, listener);
                    zookeeper.subscribeChildChanges(path, listener);
                    future.complete();
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            log("watchChildren"));
        return this;
    }

    @Override
    public Zk unwatchChildren(String path) {
        workerPool().executeBlocking(
            future -> {
                try {
                    IZkChildListener listener = childWatches.remove(path);
                    if (listener != null) {
                        zookeeper.unsubscribeChildChanges(path, listener);
                    }
                    future.complete();
                } catch (Throwable t) {
                    future.fail(t);
                }
            },
            log("unwatchChildren"));
        return this;
    }

}
