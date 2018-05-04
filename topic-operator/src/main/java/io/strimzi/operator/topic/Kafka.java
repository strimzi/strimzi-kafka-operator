/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.Set;

/**
 * A DAO for interacting with the Kafka AdminClient and/or command line Kafka
 * management tools.
 */
public interface Kafka {

    /**
     * Asynchronously create the given topic in Kafka. Invoke the given
     * handler with the result. If the operation fails the given handler
     * will be called with a failed AsyncResult whose {@code cause()} is the
     * KafkaException (not an ExecutionException).
     */
    void createTopic(Topic newTopic, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously delete the given topic in Kafka. Invoke the given
     * handler with the result. If the operation fails the given handler
     * will be called with a failed AsyncResult whose {@code cause()} is the
     * KafkaException (not an ExecutionException).
     */
    void deleteTopic(TopicName topicName, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously update the topic config in Kafka. Invoke the given
     * handler with the result. If the operation fails the given handler
     * will be called with a failed AsyncResult whose {@code cause()} is the
     * KafkaException (not an ExecutionException).
     */
    void updateTopicConfig(Topic topic, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously increase the topic's partitions in Kafka. Invoke the given
     * handler with the result. If the operation fails the given handler
     * will be called with a failed AsyncResult whose {@code cause()} is the
     * KafkaException (not an ExecutionException).
     */
    void increasePartitions(Topic topic, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously change the topic's replication factor in Kafka. Invoke the given
     * handler with the result. If the operation fails the given handler
     * will be called with a failed AsyncResult whose {@code cause()} is the
     * KafkaException (not an ExecutionException).
     */
    void changeReplicationFactor(Topic topic, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously fetch the topic metadata in Kafka. Invoke the given
     * handler with the result. If the operation fails the given handler
     * will be called with a failed AsyncResult whose {@code cause()} is the
     * KafkaException (not an ExecutionException).
     * If the topic does not exist the {@link AsyncResult#result()} will be null.
     */
    void topicMetadata(TopicName topicName, Handler<AsyncResult<TopicMetadata>> handler);

    /**
     * Asynchronously list the topics available in Kafka. Invoke the given
     * handler with the result. If the operation fails the given handler
     * will be called with a failed AsyncResult whose {@code cause()} is the
     * KafkaException (not an ExecutionException).
     */
    void listTopics(Handler<AsyncResult<Set<String>>> handler);

}

