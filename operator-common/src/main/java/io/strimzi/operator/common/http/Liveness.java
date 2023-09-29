/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.http;

/**
 * A liveness check implemented by an application (e.g. an operator) and called by the
 * {@link HealthCheckAndMetricsServer} when handling a health check request.
 */
public interface Liveness {

    /**
     * Indicates whether the application is alive or not.
     * What counts as "alive" depends on the application.
     * For example, in an operator it might be simply that all controller threads are alive, or
     * it might be that they've returned to their outer loop within some timeout.
     * This method is invoked on the HTTP request handling thread so excessive blocking should be avoided.
     *
     * @return  True when the application is alive, false otherwise.
     */
    boolean isAlive();
}
