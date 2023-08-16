/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.tracing.agent;

/**
 * A very simple Java agent which initializes the distributed tracing if requested
 */
public class TracingAgent {

    /**
     * Agent entry point
     *
     * @param agentArgs The type of tracing which should be initialized
     */
    public static void premain(String agentArgs) {
        Tracing tracing;
        switch (agentArgs) {
            case "opentelemetry":
                tracing = new OpenTelemetryTracing();
                break;
            default:
                // assuming no distributed tracing by default
                tracing = new Tracing.NoTracing();
                break;
        }
        tracing.initialize();
    }
}