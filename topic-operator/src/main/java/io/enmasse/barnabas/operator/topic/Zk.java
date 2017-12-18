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
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.List;

/**
 * A vert.X-style ZooKeeper client interface.
 */
public interface Zk {

    public static Zk create(Vertx vertx, String zkConnectionString, int sessionTimeout) {
        return new ZkImpl(vertx, zkConnectionString, sessionTimeout);
    }

    Zk connect(Handler<AsyncResult<Zk>> handler);

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
     * Asynchronously create the node with the given path and data,
     * invoking the given handler with the result.
     */
    Zk create(String path, byte[] data, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously set the data in the znode at the given path to the
     * given data, invoking the given handler with the result.
     */
    Zk setData(String path, byte[] data, Handler<AsyncResult<Void>> handler);

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

}

