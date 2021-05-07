/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Base abstract class for a ZooKeeper watcher for child znodes
 */
public abstract class ZkWatcher {

    protected Logger log = LogManager.getLogger(getClass());
    protected ReconciliationLogger reconciliationLogger = new ReconciliationLogger(log);

    protected final TopicOperator topicOperator;
    private volatile ZkWatcherState state = ZkWatcherState.NOT_STARTED;
    private volatile Zk zk;

    private final ConcurrentHashMap<String, Boolean> children = new ConcurrentHashMap<>();
    private final String rootZNode;

    /**
     * Constructor
     *
     * @param topicOperator    Operator instance to notify
     * @param rootZNode     root znode to watch children
     */
    ZkWatcher(TopicOperator topicOperator, String rootZNode) {
        this.topicOperator = topicOperator;
        this.rootZNode = rootZNode;
    }

    /**
     * Start the watcher
     *
     * @param zk    Zookeeper client instance
     */
    protected void start(Zk zk) {
        this.zk = zk;
        this.state = ZkWatcherState.STARTED;
    }

    /**
     * Stop the watcher
     */
    protected void stop() {
        this.state = ZkWatcherState.STOPPED;
    }

    /**
     * @return  if the watcher is already started
     */
    protected boolean started() {
        return this.state == ZkWatcherState.STARTED;
    }

    /**
     * Add a child to watch under the root znode
     *
     * @param child child to watch
     */
    protected void addChild(String child) {
        this.children.put(child, false);
        String path = getPath(child);
        log.debug("Watching znode {} for changes", path);
        Handler<AsyncResult<byte[]>> handler = dataResult -> {
            if (dataResult.succeeded()) {
                this.children.compute(child, (k, v) -> {
                    if (v != null && v) {
                        this.notifyOperator(child);
                    }
                    return true;
                });
            } else {
                log.error("While getting or watching znode {}", path, dataResult.cause());
            }
        };
        zk.watchData(path, handler).compose(zk2 -> {
            zk.getData(path, handler);
            return Future.succeededFuture();
        });
    }

    /**
     * Remove a child from watching
     *
     * @param child child to unwatch
     */
    protected void removeChild(String child) {
        log.debug("Unwatching znode {} for changes", child);
        this.children.remove(child);
        zk.unwatchData(getPath(child));
    }

    /**
     * Return the path of the watched topic
     *
     * @param child child to get the path
     * @return  full path of the znode child
     */
    protected String getPath(String child) {
        return this.rootZNode + "/" + child;
    }

    /**
     * Check if the provided child is currently watched
     *
     * @param child child to check
     * @return  If the passed child is currently watched
     */
    protected boolean watching(String child) {
        return this.children.containsKey(child);
    }

    /**
     * Notify the operator about changes in the provided child
     *
     * @param child child changed
     */
    protected abstract void notifyOperator(String child);

    /**
     * Possible state of a ZkWatcher
     */
    enum ZkWatcherState {
        NOT_STARTED,    // = 0
        STARTED,       // = 1
        STOPPED         // = 2
    }
}
