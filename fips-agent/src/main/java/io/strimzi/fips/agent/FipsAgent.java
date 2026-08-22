/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.fips.agent;

/**
 * Java agent entry point for FIPS workarounds. Must run before Kafka classes load.
 */
public class FipsAgent {
    private FipsAgent() { }

    /**
     * Agent entry point
     * @param agentArgs The agent arguments (unused)
     */
    public static void premain(String agentArgs) {
        FipsDsaWorkaround.apply();
    }
}
