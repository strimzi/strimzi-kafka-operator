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

package io.strimzi.controller.topic;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Implementation of {@link TopicStore} that stores the topic state in ZooKeeper.
 */
public class ZkTopicStore implements TopicStore, Handler<AsyncResult<ZooKeeper>> {

    private final static Logger logger = LoggerFactory.getLogger(ZkTopicStore.class);
    private final Vertx vertx;

    private volatile ZooKeeper zookeeper = null;

    private final ACL acl;

    public ZkTopicStore(Vertx vertx) {
        this.vertx = vertx;
        acl = new ACL();
        String scheme = "world";
        String id = "anyone";
        int perm = ZooDefs.Perms.READ | ZooDefs.Perms.WRITE |ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE;
        acl.setId(new Id(scheme, id));
        acl.setPerms(perm);

    }

    private ZooKeeper getZookeeper() {
        try {
            // TODO this is the wrong way to do it: Rather than consuming
            // a ZooKeeper we should register a handler with the bootstrap
            // watcher which will be called when it has a viable session
            return zookeeper;
        } catch (Exception e) {
            logger.error("Error waiting for a ZooKeeper instance", e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException)e;
            }
            throw new RuntimeException(e);
        }
    }

    private void createParent(String path) {
        try {
            getZookeeper().create(path, new byte[0], Collections.singletonList(acl), CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // That's fine then
        } catch (KeeperException e) {
            logger.error("Error creating {}", path, e);
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            logger.error("Error creating {}", path, e);
            throw new RuntimeException(e);
        }
    }


    private static String getTopicPath(TopicName name) {
        return "/barnabas/topics/" + name;
    }

    @Override
    public void read(TopicName topicName, Handler<AsyncResult<Topic>> handler) {
        String topicPath = getTopicPath(topicName);
        logger.debug("read znode {}", topicPath);
        getZookeeper().getData(topicPath, null, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                Exception exception = mapExceptions(rc);
                final Future<Topic> future;
                if (exception instanceof NoSuchEntityExistsException) {
                    future = Future.succeededFuture(null);
                } else if (exception == null) {
                    Topic topic = TopicSerialization.fromJson(data);
                    future = Future.succeededFuture(topic);
                } else {
                    future = Future.failedFuture(exception);
                }
                logger.debug("read znode {} result: {}", topicPath, future);
                vertx.runOnContext(ar -> handler.handle(future));
            }
        }, null);
    }

    @Override
    public void create(Topic topic, Handler<AsyncResult<Void>> handler) {
        Throwable t= new Throwable();
        byte[] data = TopicSerialization.toJson(topic);
        String topicPath = getTopicPath(topic.getTopicName());
        logger.debug("create znode {}", topicPath);
        getZookeeper().create(topicPath, data,
                Collections.singletonList(acl),
                CreateMode.PERSISTENT,
                (rc, path, ctx, name) -> handleOnContext("create", path, rc, handler), null);
    }

    @Override
    public void update(Topic topic, Handler<AsyncResult<Void>> handler) {
        byte[] data = TopicSerialization.toJson(topic);
        // TODO pass a non-zero version
        String topicPath = getTopicPath(topic.getTopicName());
        logger.debug("update znode {}", topicPath);
        getZookeeper().setData(topicPath, data, -1,
                (rc, path, ctx, stat) -> handleOnContext("update", path, rc, handler), null);
    }

    @Override
    public void delete(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        // TODO pass a non-zero version
        String topicPath = getTopicPath(topicName);
        logger.debug("delete znode {}", topicPath);
        getZookeeper().delete(topicPath, -1,
                (rc, path, ctx) -> handleOnContext("delete", path, rc, handler), null);
    }

    /** Run the handler on the vertx context */
    private void handleOnContext(String action, String path, int rc, Handler<AsyncResult<Void>> handler) {
        Exception exception = mapExceptions(rc);
        Future<Void> future;
        if (exception == null) {
            future = Future.succeededFuture();
        } else {
            future = Future.failedFuture(exception);
        }
        logger.debug("{} znode {} result: {}", path, future);
        vertx.runOnContext(ar ->
                handler.handle(future)
        );
    }

    private Exception mapExceptions(int rc) {
        Exception exception;
        KeeperException.Code code = KeeperException.Code.get(rc);
        switch (code) {
            case OK:
                exception = null;
                break;
            case NODEEXISTS:
                exception = new EntityExistsException();
                break;
            case NONODE:
                exception = new NoSuchEntityExistsException();
                break;
            default:
                exception = KeeperException.create(code);

        }
        return exception;
    }

    @Override
    public void handle(AsyncResult<ZooKeeper> event) {
        this.zookeeper = event.result();
        createParent("/barnabas");
        createParent("/barnabas/topics");
    }
}
