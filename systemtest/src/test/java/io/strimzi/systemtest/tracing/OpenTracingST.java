/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.strimzi.api.kafka.model.tracing.JaegerTracing;
import io.strimzi.api.kafka.model.tracing.Tracing;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.ParallelSuite;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import static io.strimzi.systemtest.Constants.ACCEPTANCE;
import static io.strimzi.systemtest.Constants.BRIDGE;
import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.INTERNAL_CLIENTS_USED;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER;
import static io.strimzi.systemtest.Constants.MIRROR_MAKER2;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TRACING;

/**
 * Provides test cases for Jaeger OpenTracing feature. The procedure for each test case in general follows this pattern:
 *  (a) we deploy specific component with Tracing enabled,
 *  (b) then we produce/consume messages,
 *  (c) finally, we verify that we could see related data in the Jaeger API
 */
@Tag(REGRESSION)
@Tag(TRACING)
@Tag(INTERNAL_CLIENTS_USED)
@ParallelSuite
public class OpenTracingST extends TracingAbstractST {
    @Override
    protected Tracing tracing() {
        return new JaegerTracing();
    }

    @Override
    protected String serviceNameEnvVar() {
        return TracingConstants.JAEGER_SERVICE_ENV;
    }

    @ParallelNamespaceTest
    @Tag(ACCEPTANCE)
    void testProducerConsumerStreamsService(final ExtensionContext extensionContext) {
        doTestProducerConsumerStreamsService(extensionContext);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER2)
    void testProducerConsumerMirrorMaker2Service(final ExtensionContext extensionContext) {
        doTestProducerConsumerMirrorMaker2Service(extensionContext);
    }

    @ParallelNamespaceTest
    @Tag(MIRROR_MAKER)
    void testProducerConsumerMirrorMakerService(final ExtensionContext extensionContext) {
        doTestProducerConsumerMirrorMakerService(extensionContext);
    }

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerStreamsConnectService(final ExtensionContext extensionContext) {
        doTestProducerConsumerStreamsConnectService(extensionContext);
    }

    @Tag(BRIDGE)
    @ParallelNamespaceTest
    void testKafkaBridgeService(final ExtensionContext extensionContext) {
        doTestKafkaBridgeService(extensionContext);
    }

    @Tag(BRIDGE)
    @ParallelNamespaceTest
    void testKafkaBridgeServiceWithHttpTracing(final ExtensionContext extensionContext) {
        doTestKafkaBridgeServiceWithHttpTracing(extensionContext);
    }
}
