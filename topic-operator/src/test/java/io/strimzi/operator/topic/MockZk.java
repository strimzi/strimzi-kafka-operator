/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MockZk implements Zk {

    public AsyncResult<Void> createResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".connectResult");
    public AsyncResult<Void> setDataResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".setDataResult");
    public AsyncResult<List<String>> childrenResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".childrenResult");
    public AsyncResult<byte[]> dataResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName() + ".dataResult");
    private Handler<AsyncResult<List<String>>> childrenHandler;
    private Map<String, Handler<AsyncResult<byte[]>>> dataHandlers = new HashMap<>();

    public void triggerChildren(AsyncResult<List<String>> childrenResult) {
        if (childrenHandler != null) {
            childrenHandler.handle(childrenResult);
        }
    }

    public void triggerData(String path, AsyncResult<byte[]> dataResult) {
        Handler<AsyncResult<byte[]>> asyncResultHandler = dataHandlers.get(path);
        if (asyncResultHandler != null) {
            asyncResultHandler .handle(dataResult);
        }
    }

    @Override
    public Zk disconnect(Handler<AsyncResult<Void>> handler) {
        handler.handle(Future.succeededFuture());
        return this;
    }

    @Override
    public Zk create(String path, byte[] data, List<ACL> acls, CreateMode createMode, Handler<AsyncResult<Void>> handler) {
        handler.handle(createResult);
        return this;
    }

    @Override
    public Zk setData(String path, byte[] data, int version, Handler<AsyncResult<Void>> handler) {
        handler.handle(setDataResult);
        return this;
    }

    @Override
    public Zk children(String path, Handler<AsyncResult<List<String>>> handler) {
        handler.handle(childrenResult);
        return this;
    }

    @Override
    public Future<Zk> watchChildren(String path, Handler<AsyncResult<List<String>>> watcher) {
        childrenHandler = watcher;
        return Future.succeededFuture(this);
    }

    @Override
    public Zk unwatchChildren(String path) {
        childrenHandler = null;
        return this;
    }

    @Override
    public Zk getData(String path, Handler<AsyncResult<byte[]>> handler) {
        handler.handle(dataResult);
        return this;
    }

    @Override
    public Future<Zk> watchData(String path, Handler<AsyncResult<byte[]>> watcher) {
        dataHandlers.put(path, watcher);
        return Future.succeededFuture(this);
    }

    @Override
    public Zk unwatchData(String path) {
        dataHandlers.remove(path);
        return this;
    }

    @Override
    public Zk delete(String path, int version, Handler<AsyncResult<Void>> handler) {
        return null;
    }

    @Override
    public Future<Boolean> pathExists(String path) {
        return Future.succeededFuture(getPathExists(path));
    }

    @Override
    public boolean getPathExists(String path) {
        return false;
    }

    @Override
    public List<String> getChildren(String path) {
        return Collections.emptyList();
    }

    @Override
    public byte[] getData(String path) {
        return new byte[0];
    }

    @Override
    public void delete(String path, int version) {
    }
}
