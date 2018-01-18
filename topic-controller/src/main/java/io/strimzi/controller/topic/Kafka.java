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

