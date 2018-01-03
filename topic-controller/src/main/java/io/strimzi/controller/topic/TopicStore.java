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

/**
 * Represents a persistent data store where the controller can store its copy of the
 * topic state that won't be modified by either K8S or Kafka.
 */
public interface TopicStore {

    public static class EntityExistsException extends Exception {

    }

    public static class NoSuchEntityExistsException extends Exception {

    }

    /**
     * Asynchronously get the topic with the given name
     * and run the given handler on the context with the resulting Topic.
     * If no topic with the given name exists, the handler will be called with
     * a null result.
     */
    void read(TopicName name, Handler<AsyncResult<Topic>> handler);

    /**
     * Asynchronously persist the given topic in the store
     * and run the given handler on the context when done.
     * If a topic with the given name already exists, the handler will be called with
     * a failed result whose {@code cause()} is
     * {@link EntityExistsException}.
     */
    void create(Topic topic, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously update the given topic in the store
     * and run the given handler on the context when done.
     * If no topic with the given name exists, the handler will be called with
     * a failed result whose {@code cause()} is
     * {@link NoSuchEntityExistsException}.
     */
    void update(Topic topic, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously delete the given topic from the store
     * and run the given handler on the context when done.
     * If no topic with the given name exists, the handler wiil be called with
     * a failed result whose {@code cause()} is
     * {@link NoSuchEntityExistsException}.
     */
    void delete(TopicName topic, Handler<AsyncResult<Void>> handler);
}

