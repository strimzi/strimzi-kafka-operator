/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.zk;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * A vert.X-style ZooKeeper client interface.
 */
public interface Zk {


    /**
     * Creates the zookeeper client asynchronously
     *
     * @param vertx  Instance of Vert.x
     * @param zkConnectionString  Zookeeper connection string
     * @param sessionTimeout  Timeout for session
     * @param connectionTimeout Timeout for connection
     * @return  Future completes if the Zookeeper instance is created successfully.
     */
    static Future<Zk> create(Vertx vertx, String zkConnectionString, int sessionTimeout, int connectionTimeout) {
        return vertx.executeBlocking(f -> {
            try {
                f.complete(createSync(vertx, zkConnectionString, sessionTimeout, connectionTimeout));
            } catch (Throwable t) {
                f.fail(t);
            }
        });
    }

    /**
     * Creates the zookeeper client
     *
     * @param vertx  Instance of Vert.x
     * @param zkConnectionString  Zookeeper connection string
     * @param sessionTimeout  Timeout for session
     * @param connectionTimeout Timeout for connection
     * @return Zookeeper instace
     */
    static Zk createSync(Vertx vertx, String zkConnectionString, int sessionTimeout, int connectionTimeout) {
        return new ZkImpl(vertx,
                new ZkClient(zkConnectionString, sessionTimeout, connectionTimeout,
                        new BytesPushThroughSerializer()));
    }

    /**
     * Disconnect from the ZooKeeper server, asynchronously.
     *
     * @param handler The result handler.
     * @return This instance.
     */
    Zk disconnect(Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously create the znode at the given path and with the given data and ACL, using the
     * given createMode, then invoke the given handler with the result.
     *
     * @param path       The path.
     * @param data       The data.
     * @param acls       The ACLs.
     * @param createMode The create mode.
     * @param handler    The result handler.
     * @return This instance.
     */
    Zk create(String path, byte[] data, List<ACL> acls, CreateMode createMode, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously delete the znode at the given path, iff the given version is -1 or matches the version of the znode,
     * then invoke the given handler with the result.
     *
     * @param path    The path.
     * @param version The version.
     * @param handler The result handler.
     * @return This instance.
     */
    Zk delete(String path, int version, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously set the data in the znode at the given path to the
     * given data iff the given version is -1, or matches the version of the znode,
     * then invoke the given handler with the result.
     *
     * @param path    The path.
     * @param data    The data.
     * @param version The version.
     * @param handler The result handler.
     * @return This instance.
     */
    Zk setData(String path, byte[] data, int version, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously fetch the children of the znode at the given {@code path}, calling the given
     * handler with the result.
     *
     * @param path    The path.
     * @param handler The result handler.
     * @return This instance.
     */
    Zk children(String path, Handler<AsyncResult<List<String>>> handler);

    /**
     * Asynchronously set given the children {@code watcher} on the given {@code path},
     * returning a future which completes when the watcher is subscribed.
     * A subsequent call to {@link #children(String, Handler)} with the same path will register the child {@code watcher}
     * for the given {@code path} current at that time with zookeeper so
     * that that {@code watcher} is called when the children of the given {@code path} change.
     *
     * @param path    The path.
     * @param watcher The watcher.
     * @return This instance.
     */
    Future<Zk> watchChildren(String path, Handler<AsyncResult<List<String>>> watcher);

    /**
     * Remove the children watcher, if any, for the given {@code path}.
     *
     * @param path The path.
     * @return This instance.
     */
    Zk unwatchChildren(String path);

    /**
     * Asynchronously fetch the data of the given znode at the given path, calling the given handler
     * with the result.
     *
     * @param path    The path.
     * @param handler The result handler.
     * @return This instance.
     */
    Zk getData(String path, Handler<AsyncResult<byte[]>> handler);

    /**
     * Asynchronously set given the data {@code watcher} on the given {@code path},
     * returning a future which completes when the watcher is subscribed.
     * A subsequent call to {@link #getData(String, Handler)} with the same path will register the data {@code watcher}
     * for the given {@code path} current at that time with zookeeper so
     * that that {@code watcher} is called when the data of the given {@code path} changes.
     *
     * @param path    The path.
     * @param watcher The result handler.
     * @return This instance
     */
    Future<Zk> watchData(String path, Handler<AsyncResult<byte[]>> watcher);

    /**
     * Remove the data watcher, if any, for the given {@code path}.
     *
     * @param path The path.
     * @return This instance.
     */
    Zk unwatchData(String path);

    /**
     * Does the path exist.
     *
     * @param path The path
     * @return Future with the result.
     */
    Future<Boolean> pathExists(String path);

    // sync methods -- use in already async code

    /**
     * Does the path exist.
     *
     * @param path The path
     * @return boolean result
     */
    boolean getPathExists(String path);

    /**
     * List of child nodes
     *
     * @param path  Topic path
     * @return List of child nodes
     */
    List<String> getChildren(String path);

    /**
     * Gets byte form of zk data at topic path
     *
     * @param path  Topic path
     * @return Byte stream of data
     * */
    byte[] getData(String path);

    /**
     * Deletes zk topic path
     *
     * @param path     Topic Path
     * @param version  version
     * */
    void delete(String path, int version);

    // TODO getAcl(), setAcl(), multi()

}

