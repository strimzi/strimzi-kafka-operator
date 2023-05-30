/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.http;

/**
 * A readiness check implemented by an application (e.g. an operator) and called by the
 * {@link HealthCheckAndMetricsServer} when handling a health check request.
 */
public interface Readiness {

    /**
     * Indicates whether the application is ready.
     * What counts as "ready" depends on the application.
     * For example, in an operator it might be that all controller threads are running.
     * This method is invoked on the HTTP request handling thread so excessive blocking should be avoided.
     *
     * @return  True when the application is ready, false otherwise
     */
    boolean isReady();
}
