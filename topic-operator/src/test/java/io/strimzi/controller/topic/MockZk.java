/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.strimzi.controller.topic.zk.Zk;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

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

    public void triggerData(AsyncResult<byte[]> dataResult) {
        if (!dataHandlers.isEmpty()) {
            for (Handler<AsyncResult<byte[]>> handler: dataHandlers.values()) {
                handler.handle(dataResult);
            }
        }
    }

    @Override
    public Zk disconnect() {
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
    public Zk watchChildren(String path, Handler<AsyncResult<List<String>>> watcher) {
        childrenHandler = watcher;
        return this;
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
    public Zk watchData(String path, Handler<AsyncResult<byte[]>> watcher) {
        dataHandlers.put(path, watcher);
        return this;
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
    public Zk watchExists(String path, Handler<AsyncResult<Stat>> watcher) {
        return null;
    }

    @Override
    public Zk unwatchExists(String path) {
        return null;
    }

    @Override
    public Zk exists(String path, Handler<AsyncResult<Stat>> handler) {
        return null;
    }
}
