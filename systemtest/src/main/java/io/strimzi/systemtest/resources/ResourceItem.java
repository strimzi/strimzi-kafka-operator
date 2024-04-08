/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Represents a generic resource item within the test framework.
 * This class encapsulates a Kubernetes resource along with a {@link ThrowableRunner}
 * that defines how to clean up or manipulate the resource.
 *
 * @param <T> The type of Kubernetes resource, which must implement {@link HasMetadata}.
 */
public final class ResourceItem<T extends HasMetadata> {

    private final ThrowableRunner throwableRunner;
    private final T resource;

    /**
     * Constructs a new ResourceItem with both a throwable runner and a resource.
     *
     * @param throwableRunner A {@link ThrowableRunner} instance used for performing operations on the resource.
     * @param resource        The Kubernetes resource of type T.
     */
    public ResourceItem(ThrowableRunner throwableRunner, T resource) {
        this.throwableRunner = throwableRunner;
        this.resource = resource;
    }

    /**
     * Constructs a new ResourceItem with only a throwable runner, without specifying a resource.
     * This constructor can be used when the operation does not require direct access to the resource.
     *
     * @param throwableRunner A {@link ThrowableRunner} instance used for performing operations.
     */
    public ResourceItem(ThrowableRunner throwableRunner) {
        this(throwableRunner, null);
    }

    /**
     * Returns the {@link ThrowableRunner} associated with this resource item.
     *
     * @return The {@link ThrowableRunner} for this resource item.
     */
    public ThrowableRunner getThrowableRunner() {
        return throwableRunner;
    }

    /**
     * Returns the Kubernetes resource of type T.
     *
     * @return The Kubernetes resource, or null if not specified.
     */
    public T getResource() {
        return resource;
    }
}