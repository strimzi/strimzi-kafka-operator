/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.api.model.HasMetadata;

public interface ResourceType<T extends HasMetadata> {
    String getKind();

    T get(String namespace, String name);

    void create(T resource);

    void delete(T resource) throws Exception;

    /**
     * Check if this resource is marked as ready or not.
     *
     * @return true if ready.
     */
    boolean isReady(T resource);

    /**
     * Update the resource with the latest state on the Kubernetes API.
     */
    void refreshResource(T existing, T newResource);
}
