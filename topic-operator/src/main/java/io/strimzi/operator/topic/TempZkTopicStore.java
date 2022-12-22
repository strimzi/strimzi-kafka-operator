/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.topic.zk.Zk;
import io.vertx.core.Future;

/**
 * Implementation of {@link TopicStore} that stores the topic state in ZooKeeper.,
 * its synchronous version.
 */
class TempZkTopicStore implements TopicStore {
    private final String topicsPath;
    private final Zk zk;

    public TempZkTopicStore(Zk zk, String topicsPath) {
        this.zk = zk;
        this.topicsPath = topicsPath;
    }

    private String getTopicPath(TopicName name) {
        return topicsPath + "/" + name;
    }

    @Override
    public Future<Topic> read(TopicName topicName) {
        String topicPath = getTopicPath(topicName);
        byte[] bytes = zk.getData(topicPath);
        Topic topic = TopicSerialization.fromJson(bytes);
        return Future.succeededFuture(topic);
    }

    /**
     * Creates the topic store which store topic state in Zookeeper
     *
     * @param topic    The topic resource
     * @return  A failed future
     */
    @Override
    public Future<Void> create(Topic topic) {
        return Future.failedFuture("Not supported!");
    }

    /**
     * Updates the topic store which store topic state in Zookeeper
     *
     * @param topic    The topic resource
     * @return  A failed future
     */
    @Override
    public Future<Void> update(Topic topic) {
        return Future.failedFuture("Not supported!");
    }

    /**
     * Deletes the topic store which store topic state in Zookeeper
     *
     * @param topicName    The topic name
     * @return  A failed future
     */
    @Override
    public Future<Void> delete(TopicName topicName) {
        String topicPath = getTopicPath(topicName);
        zk.delete(topicPath, -1);
        return Future.succeededFuture();
    }
}
