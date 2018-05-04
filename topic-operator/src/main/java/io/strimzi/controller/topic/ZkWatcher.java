/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.topic;

import io.strimzi.controller.topic.zk.Zk;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Base abstract class for a ZooKeeper watcher for child znodes
 */
public abstract class ZkWatcher {

    protected Logger log = LoggerFactory.getLogger(getClass());

    protected final Controller controller;
    private volatile ZkWatcherState state = ZkWatcherState.NOT_STARTED;
    private volatile Zk zk;

    private final ConcurrentHashMap<String, Boolean> children = new ConcurrentHashMap<>();
    private final String rootZNode;

    /**
     * Constructor
     *
     * @param controller    Controller instance to notify
     * @param rootZNode     root znode to watch children
     */
    ZkWatcher(Controller controller, String rootZNode) {
        this.controller = controller;
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
                    if (v) {
                        this.notifyController(child);
                    }
                    return true;
                });
            } else {
                log.error("While getting or watching znode {}", path, dataResult.cause());
            }
        };
        zk.watchData(path, handler).getData(path, handler);
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
     * Notify the controller about changes in the provided child
     *
     * @param child child changed
     */
    protected abstract void notifyController(String child);

    /**
     * Possible state of a ZkWatcher
     */
    enum ZkWatcherState {
        NOT_STARTED,    // = 0
        STARTED,       // = 1
        STOPPED         // = 2
    }
}
