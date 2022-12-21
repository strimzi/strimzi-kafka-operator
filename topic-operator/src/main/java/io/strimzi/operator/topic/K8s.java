/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.Event;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;

import java.util.List;

/** K8s Interface */
public interface K8s {

    /**
     * Asynchronously create the given resource.
     *
     * @param topicResource The resource to be created.
     * @return A future which completes when the topic has been created.
     */
    Future<KafkaTopic> createResource(KafkaTopic topicResource);

    /**
     * Asynchronously update the given resource.
     *
     * @param topicResource The topic.
     * @return A future which completes when the topic has been updated.
     */
    Future<KafkaTopic> updateResource(KafkaTopic topicResource);

    /**
     * Asynchronously update the given resource's status.
     *
     * @param reconciliation The reconciliation
     * @param topicResource The topic.
     * @return A future which completes when the topic's status has been updated.
     */
    Future<KafkaTopic> updateResourceStatus(Reconciliation reconciliation, KafkaTopic topicResource);

    /**
     * Asynchronously delete the given resource.
     *
     * @param reconciliation The reconciliation
     * @param resourceName The name of the resource to be deleted.
     * @return A future which completes when the topic has been deleted.
     */
    Future<Void> deleteResource(Reconciliation reconciliation, ResourceName resourceName);

    /**
     * Asynchronously list the resources.
     * @return A future which completes with the topics.
     */
    Future<List<KafkaTopic>> listResources();

    /**
     * Get the resource with the given name, invoking the given handler with the result.
     * If a resource with the given name does not exist, the handler will be called with
     * a null {@link AsyncResult#result() result()}.
     *
     * @param resourceName The name of the resource to get.
     * @return A future which completes with the topic
     */
    Future<KafkaTopic> getFromName(ResourceName resourceName);

    /**
     * Create an event.
     *
     * @param event The event.
     * @return A future which completes when the event has been created.
     */
    Future<Void> createEvent(Event event);
}
