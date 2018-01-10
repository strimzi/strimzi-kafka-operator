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
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * A vert.X-style ZooKeeper client interface.
 */
public interface Zk {

    public static Zk create(Vertx vertx, String zkConnectionString, int sessionTimeout) {
        return new ZkImpl(vertx, zkConnectionString, sessionTimeout);
    }

    Zk connect(Handler<AsyncResult<Zk>> handler);

    @Deprecated
    Zk temporaryConnectionHandler(Handler<AsyncResult<ZooKeeper>> handler);

    /**
     * Register a handler to be called when the client gets disconnected from
     * the zookeeper server/cluster. If the disconnection was caused explicitly
     * via {@link #disconnect(Handler)} the {@code handler}'s result will be
     * null, otherwise if the connection was lost for any other reason the
     * {@code handler}'s result will be the Zk instance.
     *
     * The disconnection handler can be used to automatically reconnect
     * to the server if the connection gets lost.
     */
    Zk disconnectionHandler(Handler<AsyncResult<Zk>> handler);

    /**
     * Explicitly disconnect from the connected zookeeper.
     * Any configured {@link #disconnectionHandler(Handler)} will be
     * invoked with a null result and then the given handler will be invoked.
     */
    Zk disconnect(Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously create the node at the given path and with the given data and ACL, using the
     * given createMode, then invoke the given handler with the result.
     */
    Zk create(String path, byte[] data, List<ACL> acls, CreateMode createMode, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously set the data in the znode at the given path to the
     * given data iff the given version is -1, or matches the version of the znode,
     * then invoke the given handler with the result.
     */
    Zk setData(String path, byte[] data, int version, Handler<AsyncResult<Void>> handler);

    Zk children(String path, Handler<AsyncResult<List<String>>> handler);

    /**
     * Set given the watcher on the given path.
     * A subsequent call to {@link #children(String, Handler)} with the same path will register the child watcher
     * for the given path current at that time with zookeeper so
     * that that watcher is called when the children of the given path change.
     * @param path
     * @param watcher
     * @return
     */
    Zk watchChildren(String path, Handler<AsyncResult<List<String>>> watcher);

    /**
     * Remove any children watcher for the given path.
     */
    Zk unwatchChildren(String path);


    Zk getData(String path, Handler<AsyncResult<byte[]>> handler);
    Zk watchData(String path, Handler<AsyncResult<byte[]>> watcher);
    Zk unwatchData(String path);

    /**
     * Delete the znode at the given path, iff the given version is -1 or matches the version of the znode,
     * then invoke the given handler with the result.
     */
    Zk delete(String path, int version, Handler<AsyncResult<Void>> handler);

    /**
     * Check for the existence of the znode at the given {@code path},
     * calling the the given {@code complete} with the existence result,
     * and the given {@code watcher} whenever the existence subsequently changes.
     */
    Zk watchExists(String path, Handler<AsyncResult<Stat>> watcher);

    /**
     * Remove the given {@code watcher} that was previously added (via one of the {@code watchExists() methods}
     * for watching existence changes to the given path.
     * It is not an error if the given watcher was not actually watching the given path.
     */
    Zk unwatchExists(String path);

    /**
     * Check whether a znode exists at the given path.
     */
    Zk exists(String path, Handler<AsyncResult<Stat>> handler);

    // TODO getAcl(), setAcl(), multi()

}

