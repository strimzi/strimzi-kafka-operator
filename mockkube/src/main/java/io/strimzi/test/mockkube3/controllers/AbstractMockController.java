/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube3.controllers;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * MockKube3 uses mock controllers to emulate Kubernetes functionality which is useful for out tests but normally is
 * done by the built-in Kubernetes controllers and not by Strimzi. All controllers inherit from this abstract class and
 * implement start and stop method. In the start method, they typically create a watch to the Kubernetes cluster to
 * start watching the resources they control. The stop method is used to stop the watch.
 */
public abstract class AbstractMockController {
    /**
     * Constructs the mock Kubernetes controller
     */
    public AbstractMockController() {
    }

    /**
     * Starts the mock controller
     *
     * @param client    Kubernetes client to be used by this controller
     */
    abstract public void start(KubernetesClient client);

    /**
     * Stops the mock controller
     */
    abstract public void stop();
}
