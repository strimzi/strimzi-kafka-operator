/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Represents a persistent data store where the operator can store its copy of the
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

