/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.zk;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * A vert.X-style ZooKeeper client interface.
 */
public interface Zk {

    public static Zk create(Vertx vertx, String zkConnectionString, int sessionTimeout, int connectionTimeout) {
        return new ZkImpl(vertx, zkConnectionString, sessionTimeout, connectionTimeout);
    }

    /**
     * Disconnect from the ZooKeeper server, asynchronously.
     */
    Zk disconnect(Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously create the znode at the given path and with the given data and ACL, using the
     * given createMode, then invoke the given handler with the result.
     */
    Zk create(String path, byte[] data, List<ACL> acls, CreateMode createMode, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously delete the znode at the given path, iff the given version is -1 or matches the version of the znode,
     * then invoke the given handler with the result.
     */
    Zk delete(String path, int version, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously set the data in the znode at the given path to the
     * given data iff the given version is -1, or matches the version of the znode,
     * then invoke the given handler with the result.
     */
    Zk setData(String path, byte[] data, int version, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously fetch the children of the znode at the given {@code path}, calling the given
     * handler with the result.
     */
    Zk children(String path, Handler<AsyncResult<List<String>>> handler);

    /**
     * Set given the children {@code watcher} on the given {@code path}.
     * A subsequent call to {@link #children(String, Handler)} with the same path will register the child {@code watcher}
     * for the given {@code path} current at that time with zookeeper so
     * that that {@code watcher} is called when the children of the given {@code path} change.
     */
    Zk watchChildren(String path, Handler<AsyncResult<List<String>>> watcher);

    /**
     * Remove the children watcher, if any, for the given {@code path}.
     */
    Zk unwatchChildren(String path);

    /**
     * Asynchronously fetch the data of the given znode at the given path, calling the given handler
     * with the result.
     */
    Zk getData(String path, Handler<AsyncResult<byte[]>> handler);

    /**
     * Set given the data {@code watcher} on the given {@code path}.
     * A subsequent call to {@link #getData(String, Handler)} with the same path will register the data {@code watcher}
     * for the given {@code path} current at that time with zookeeper so
     * that that {@code watcher} is called when the data of the given {@code path} changes.
     */
    Zk watchData(String path, Handler<AsyncResult<byte[]>> watcher);

    /**
     * Remove the data watcher, if any, for the given {@code path}.
     */
    Zk unwatchData(String path);

    // TODO getAcl(), setAcl(), multi()

}

