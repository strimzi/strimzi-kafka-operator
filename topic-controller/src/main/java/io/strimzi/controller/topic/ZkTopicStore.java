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

import io.strimzi.controller.topic.zk.AclBuilder;
import io.strimzi.controller.topic.zk.AclBuilder.Permission;
import io.strimzi.controller.topic.zk.Zk;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of {@link TopicStore} that stores the topic state in ZooKeeper.
 */
public class ZkTopicStore implements TopicStore {

    private final static Logger logger = LoggerFactory.getLogger(ZkTopicStore.class);
    public static final String TOPICS_PATH = "/strimzi/topics";
    private final Vertx vertx;

    private final Zk zk;

    private final List<ACL> acl;

    public ZkTopicStore(Zk zk, Vertx vertx) {
        this.zk = zk;
        this.vertx = vertx;
        acl = new AclBuilder().addWorld(Permission.values()).build();
        createParent("/strimzi");
        createParent(TOPICS_PATH);
    }

    private void createParent(String path) {
        zk.create(path, null, acl, CreateMode.PERSISTENT, result -> {
            if (result.failed()) {
                if (!(result.cause() instanceof KeeperException.NodeExistsException)) {
                    logger.error("Error creating {}", path, result.cause());
                    throw new RuntimeException(result.cause());
                }
            }
        });
    }


    private static String getTopicPath(TopicName name) {
        return TOPICS_PATH + "/" + name;
    }

    @Override
    public void read(TopicName topicName, Handler<AsyncResult<Topic>> handler) {
        String topicPath = getTopicPath(topicName);
        logger.debug("read znode {}", topicPath);
        zk.getData(topicPath, result -> {
            final AsyncResult<Topic> fut;
            if (result.succeeded()) {
                fut = Future.succeededFuture(TopicSerialization.fromJson(result.result()));
            } else {
                if (result.cause() instanceof KeeperException.NoNodeException) {
                    fut = Future.succeededFuture(null);
                } else {
                    fut = result.map((Topic)null);
                }
            }
            handler.handle(fut);
        });
    }

    @Override
    public void create(Topic topic, Handler<AsyncResult<Void>> handler) {
        Throwable t= new Throwable();
        byte[] data = TopicSerialization.toJson(topic);
        String topicPath = getTopicPath(topic.getTopicName());
        logger.debug("create znode {}", topicPath);
        zk.create(topicPath, data, acl, CreateMode.PERSISTENT, result -> {
            if (result.failed() && result.cause() instanceof KeeperException.NodeExistsException) {
                handler.handle(Future.failedFuture(new EntityExistsException()));
            } else {
                handler.handle(result);
            }
        });
    }

    @Override
    public void update(Topic topic, Handler<AsyncResult<Void>> handler) {
        byte[] data = TopicSerialization.toJson(topic);
        // TODO pass a non-zero version
        String topicPath = getTopicPath(topic.getTopicName());
        logger.debug("update znode {}", topicPath);
        zk.setData(topicPath, data, -1, handler);
    }

    @Override
    public void delete(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        // TODO pass a non-zero version
        String topicPath = getTopicPath(topicName);
        logger.debug("delete znode {}", topicPath);
        zk.delete(topicPath, -1, result -> {
            if (result.failed() && result.cause() instanceof KeeperException.NoNodeException) {
                handler.handle(Future.failedFuture(new NoSuchEntityExistsException()));
            } else {
                handler.handle(result);
            }
        });
    }
}
