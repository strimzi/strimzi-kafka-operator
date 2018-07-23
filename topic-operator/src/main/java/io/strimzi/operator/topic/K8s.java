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

    void createResource(KafkaTopic topicResource, Handler<AsyncResult<Void>> handler);

    void updateResource(KafkaTopic topicResource, Handler<AsyncResult<Void>> handler);

    void deleteResource(ResourceName resourceName, Handler<AsyncResult<Void>> handler);

    void listMaps(Handler<AsyncResult<List<KafkaTopic>>> handler);

    /**
     * Get the resource with the given name, invoking the given handler with the result.
     * If a resource with the given name does not exist, the handler will be called with
     * a null {@link AsyncResult#result() result()}.
     */
    void getFromName(ResourceName resourceName, Handler<AsyncResult<KafkaTopic>> handler);

    void createEvent(Event event, Handler<AsyncResult<Void>> handler);
}
