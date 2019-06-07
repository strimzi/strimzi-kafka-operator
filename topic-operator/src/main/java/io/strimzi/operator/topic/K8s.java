/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

public interface K8s {

    /**
     * Asynchronously create the given resource.
     * @param topicResource The resource to be created.
     * @param handler The result handler.
     */
    void createResource(KafkaTopic topicResource, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously update the given resource.
     * @param topicResource The topic.
     * @param handler The result handler.
     */
    void updateResource(KafkaTopic topicResource, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously delete the given resource.
     * @param resourceName The name of the resource to be deleted.
     * @param handler The result handler.
     */
    void deleteResource(ResourceName resourceName, Handler<AsyncResult<Void>> handler);

    /**
     * Asynchronously list the resources.
     * @param handler The result handler.
     */
    void listMaps(Handler<AsyncResult<List<KafkaTopic>>> handler);

    /**
     * Get the resource with the given name, invoking the given handler with the result.
     * If a resource with the given name does not exist, the handler will be called with
     * a null {@link AsyncResult#result() result()}.
     * @param resourceName The name of the resource to get.
     * @param handler The result handler.
     */
    void getFromName(ResourceName resourceName, Handler<AsyncResult<KafkaTopic>> handler);

    /**
     * Create an event.
     * @param event The event.
     * @param handler The result handler.
     */
    void createEvent(Event event, Handler<AsyncResult<Void>> handler);
}
