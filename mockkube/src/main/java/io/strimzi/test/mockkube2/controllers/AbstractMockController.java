/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube2.controllers;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * MockKube2 uses mock controllers to emulate Kubernetes functionality which is useful for out tests but normally is
 * done by the built-in Kubernetes controllers and not by Strimzi. All controllers inherit from this abstract class and
 * implement start and stop method. In the start method, they typically create a watch to the Kubernetes cluster to
 * start watching the resources they control. The stop method is used to stop the watch.
 */
public abstract class AbstractMockController {
    protected final KubernetesClient client;

    /**
     * Constructs the mokc Kubernetes controller
     *
     * @param client    Kubernetes client which should be used to mock the controller behavior
     */
    public AbstractMockController(KubernetesClient client) {
        this.client = client;
    }

    /**
     * Starts the mock controller
     */
    abstract public void start();

    /**
     * Stops the mock controller
     */
    abstract public void stop();
}
