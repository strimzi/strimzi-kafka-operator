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

    /**
     * Asynchronously disconnect from the ZooKeeper server,
     * invoking the given handler when disconnected.
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

    /**
     * Asynchronously check whether a znode exists at the given {@code path},
     * calling the given handler with the result.
     * The result will be null if no znode existed at the given {@code path}.
     */
    Zk exists(String path, Handler<AsyncResult<Stat>> handler);

    /**
     * Set given the existence {@code watcher} on the given {@code path}.
     * A subsequent call to {@link #exists(String, Handler)} with the same path will register the existence {@code watcher}
     * for the given {@code path} current at that time with zookeeper so
     * that that {@code watcher} is called when a znode at that path is created or deleted.
     */
    Zk watchExists(String path, Handler<AsyncResult<Stat>> watcher);

    /**
     * Remove the existence watcher, if any, for the given {@code path}.
     */
    Zk unwatchExists(String path);

    // TODO getAcl(), setAcl(), multi()

}

