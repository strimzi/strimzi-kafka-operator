/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.zk;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link Zk}
 */
public class ZkImpl implements Zk {

    private final static Logger LOGGER = LogManager.getLogger(ZkImpl.class);
    private final WorkerExecutor workerExecutor;

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

    protected ZkImpl(Vertx vertx, ZkClient zkClient) {
        this.vertx = vertx;
        this.workerExecutor = vertx.createSharedWorkerExecutor(getClass().getName(), 4);
        this.zookeeper = zkClient;
    }


    @Override
    public Zk create(String path, byte[] data, List<ACL> acls, CreateMode createMode, Handler<AsyncResult<Void>> handler) {
        workerExecutor.executeBlocking(
            () -> {
                try {
                    zookeeper.create(path, data == null ? new byte[0] : data, acls, createMode);
                    return null;
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(e -> handler.handle(Future.succeededFuture()));
        return this;
    }

    @Override
    public Zk setData(String path, byte[] data, int version, Handler<AsyncResult<Void>> handler) {
        workerExecutor.executeBlocking(
            () -> {
                try {
                    zookeeper.writeData(path, data, version);
                    return null;
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(e -> handler.handle(Future.succeededFuture()));
        return this;
    }

    @Override
    public Zk disconnect(Handler<AsyncResult<Void>> handler) {
        workerExecutor.executeBlocking(
            () -> {
                try {
                    zookeeper.close();
                    return null;
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(e -> handler.handle(Future.succeededFuture()));
        return this;
    }

    @Override
    public Zk getData(String path, Handler<AsyncResult<byte[]>> handler) {
        workerExecutor.executeBlocking(
            () -> {
                try {
                    return (byte[]) zookeeper.readData(path);
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(ar -> handler.handle(ar));
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
    public Future<Zk> watchData(String path, Handler<AsyncResult<byte[]>> watcher) {
        Promise<Zk> result = Promise.promise();
        workerExecutor.executeBlocking(
            () -> {
                try {
                    IZkDataListener listener = new DataWatchAdapter(watcher);
                    dataWatches.put(path, listener);
                    zookeeper.subscribeDataChanges(path, listener);
                    return null;
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(ar -> {
                log("watchData").handle(ar);
                if (ar.succeeded()) {
                    result.complete(this);
                } else {
                    result.fail(ar.cause());
                }
            });
        return result.future();
    }

    @Override
    public Zk unwatchData(String path) {
        workerExecutor.executeBlocking(
            () -> {
                try {
                    IZkDataListener listener = dataWatches.remove(path);
                    if (listener != null) {
                        zookeeper.unsubscribeDataChanges(path, listener);
                    }
                    return null;
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(ar -> log("unwatchData"));
        return this;
    }

    @Override
    public Zk delete(String path, int version, Handler<AsyncResult<Void>> handler) {
        workerExecutor.executeBlocking(
            () -> {
                try {
                    if (zookeeper.delete(path, version)) {
                        return null;
                    } else {
                        throw new ZkNoNodeException();
                    }
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(ar -> handler.handle(Future.succeededFuture()));
        return this;
    }

    @Override
    public Zk children(String path, Handler<AsyncResult<List<String>>> handler) {
        workerExecutor.executeBlocking(
            () -> {
                try {
                    return zookeeper.getChildren(path);
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(ar -> handler.handle(ar));
        return this;
    }

    @Override
    public Future<Zk> watchChildren(String path, Handler<AsyncResult<List<String>>> watcher) {
        Promise<Zk> result = Promise.promise();
        workerExecutor.executeBlocking(
            () -> {
                try {
                    IZkChildListener listener = (parentPath, currentChilds) -> watcher.handle(Future.succeededFuture(currentChilds));
                    childWatches.put(path, listener);
                    return zookeeper.subscribeChildChanges(path, listener);
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(ar -> {
                log("watchChildren");
                if (ar.succeeded()) {
                    result.complete(this);
                } else {
                    result.fail(ar.cause());
                }
            });
        return result.future();
    }

    @Override
    public Zk unwatchChildren(String path) {
        workerExecutor.executeBlocking(
            () -> {
                try {
                    IZkChildListener listener = childWatches.remove(path);
                    if (listener != null) {
                        zookeeper.unsubscribeChildChanges(path, listener);
                    }
                    return null;
                } catch (Throwable t) {
                    throw t;
                }
            }).onComplete(ar -> log("unwatchChildren"));
        return this;
    }

    @Override
    public Future<Boolean> pathExists(String path) {
        Promise<Boolean> promise = Promise.promise();
        AtomicBoolean result = new AtomicBoolean(false);
        workerExecutor.executeBlocking(
            () -> {
                try {
                    result.set(getPathExists(path));
                } catch (Throwable t) {
                    result.set(false);
                }
                return result.get();
            }).onComplete(ar -> promise.complete(result.get()));
        return promise.future();
    }

    @Override
    public boolean getPathExists(String path) {
        return zookeeper.exists(path);
    }

    @Override
    public List<String> getChildren(String path) {
        return zookeeper.getChildren(path);
    }

    @Override
    public byte[] getData(String path) {
        return zookeeper.readData(path);
    }

    @Override
    public void delete(String path, int version) {
        zookeeper.delete(path, version);
    }
}
