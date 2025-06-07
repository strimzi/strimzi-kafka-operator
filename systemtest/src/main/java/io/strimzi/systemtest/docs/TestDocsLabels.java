/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.docs;

/**
 * Provides documentation labels used in {@link io.skodjob.annotations.SuiteDoc#labels()} or
 * {@link io.skodjob.annotations.TestDoc#labels()}.
 */
public interface TestDocsLabels {

    String KAFKA = "kafka";
    String BRIDGE = "bridge";
    String CONNECT = "connect";
    String CRUISE_CONTROL = "cruise-control";
    String DYNAMIC_CONFIGURATION = "dynamic-configuration";
    String LOGGING = "logging";
    String MIRROR_MAKER_2 = "mirror-maker-2";
    String METRICS = "metrics";
    String KAFKA_ACCESS = "kafka-access";
    String OLM = "olm";
}
