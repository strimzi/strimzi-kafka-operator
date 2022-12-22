/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.AclBuilder;
import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * Implementation of {@link TopicStore} that stores the topic state in ZooKeeper.
 */
public class ZkTopicStore implements TopicStore {

    private final static Logger LOGGER = LogManager.getLogger(ZkTopicStore.class);

    private final String topicsPath;

    private final Zk zk;

    private final List<ACL> acl;

    /**
     * Constructor
     *
     * @param zk          Vert.x style zookeeper instance
     * @param topicsPath  Path of the topic
     */
    public ZkTopicStore(Zk zk, String topicsPath) {
        this.zk = zk;
        this.topicsPath = topicsPath;
        acl = new AclBuilder().setWorld(AclBuilder.Permission.values()).build();
        createStrimziTopicsPath();
    }

    private void createStrimziTopicsPath() {
        zk.create("/strimzi", null, acl, CreateMode.PERSISTENT, result -> {
            if (result.failed()) {
                if (!(result.cause() instanceof ZkNodeExistsException)) {
                    LOGGER.error("Error creating {}", "/strimzi", result.cause());
                    throw new RuntimeException(result.cause());
                }
            }
            zk.create(topicsPath, null, acl, CreateMode.PERSISTENT, result2 -> {
                if (result2.failed()) {
                    if (!(result2.cause() instanceof ZkNodeExistsException)) {
                        LOGGER.error("Error creating {}", topicsPath, result2.cause());
                        throw new RuntimeException(result2.cause());
                    }
                }
            });
        });
    }


    private String getTopicPath(TopicName name) {
        return topicsPath + "/" + name;
    }

    /**
     * Read the Topic present in the zookeeper topic store
     *
     * @param topicName       Name of the topic to be deleted
     * @return Future which succeeds if the topic is read successfully
     */
    @Override
    public Future<Topic> read(TopicName topicName) {
        Promise<Topic> handler = Promise.promise();
        String topicPath = getTopicPath(topicName);
        zk.getData(topicPath, result -> {
            final AsyncResult<Topic> fut;
            if (result.succeeded()) {
                fut = Future.succeededFuture(TopicSerialization.fromJson(result.result()));
            } else {
                if (result.cause() instanceof ZkNoNodeException) {
                    fut = Future.succeededFuture(null);
                } else {
                    fut = result.map((Topic) null);
                }
            }
            handler.handle(fut);
        });
        return handler.future();
    }

    /**
     * Create the Topic in the zookeeper topic store
     *
     * @param topic       Topic to be created
     * @return Future based upon creation of the resource. Future completes if the topic is created successfully
     */
    @Override
    public Future<Void> create(Topic topic) {
        Promise<Void> handler = Promise.promise();
        byte[] data = TopicSerialization.toJson(topic);
        String topicPath = getTopicPath(topic.getTopicName());
        LOGGER.debug("create znode {}", topicPath);
        zk.create(topicPath, data, acl, CreateMode.PERSISTENT, result -> {
            if (result.failed() && result.cause() instanceof ZkNodeExistsException) {
                handler.handle(Future.failedFuture(new EntityExistsException()));
            } else {
                handler.handle(result);
            }
        });
        return handler.future();
    }

    /**
     * Update the Topic in the zookeeper topic store
     *
     * @param topic       Topic to be created
     * @return Future based upon update of the resource. Future completes if the topic is updated successfully
     */
    @Override
    public Future<Void> update(Topic topic) {
        Promise<Void> handler = Promise.promise();
        byte[] data = TopicSerialization.toJson(topic);
        // TODO pass a non-zero version
        String topicPath = getTopicPath(topic.getTopicName());
        LOGGER.debug("update znode {}", topicPath);
        zk.setData(topicPath, data, -1, handler);
        return handler.future();
    }

    /**
     * Update the Topic in the zookeeper topic store
     *
     * @param topicName       Name of the topic to be deleted
     * @return Future based upon deletion of the resource. Future completes if the topic is deleted successfully
     */
    @Override
    public Future<Void> delete(TopicName topicName) {
        Promise<Void> handler = Promise.promise();
        // TODO pass a non-zero version
        String topicPath = getTopicPath(topicName);
        LOGGER.debug("delete znode {}", topicPath);
        zk.delete(topicPath, -1, result -> {
            if (result.failed() && result.cause() instanceof ZkNoNodeException) {
                handler.handle(Future.failedFuture(new NoSuchEntityExistsException()));
            } else {
                handler.handle(result);
            }
        });
        return handler.future();
    }
}
