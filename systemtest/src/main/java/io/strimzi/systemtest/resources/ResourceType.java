/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Providing contract for all resources which must implement REST API methods for create, update (refresh) and so on.
 * @param <T> type for all our resources for instance KafkaResource, KafkaConnectResource, OlmResource, ServiceResource etc.
 */
public interface ResourceType<T extends HasMetadata> {
    String getKind();

    /**
     * Retrieve resource using Kubernetes API
     * @return specific resource with T type.
     */
    T get(String namespace, String name);

    /**
     * Creates specific resource based on T type using Kubernetes API
     */
    void create(T resource);

    /**
     * Delete specific resource based on T type using Kubernetes API
     */
    void delete(T resource) throws Exception;

    /**
     * Check if this resource is marked as ready or not with wait.
     *
     * @return true if ready.
     */
    boolean waitForReadiness(T resource);
}
