/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

/**
 * Interface for keep tracing constants used across system tests.
 */
public interface TracingConstants {

    // client and a component services
    String JAEGER_PRODUCER_SERVICE = "hello-world-producer";
    String JAEGER_CONSUMER_SERVICE = "hello-world-consumer";
    String JAEGER_KAFKA_STREAMS_SERVICE = "hello-world-streams";
    String JAEGER_MIRROR_MAKER_SERVICE = "my-mirror-maker";
    String JAEGER_MIRROR_MAKER2_SERVICE = "my-mirror-maker2";
    String JAEGER_KAFKA_CONNECT_SERVICE = "my-connect";
    String JAEGER_KAFKA_BRIDGE_SERVICE = "my-kafka-bridge";

    String JAEGER_INSTANCE_NAME = "my-jaeger";
    String JAEGER_SAMPLER_TYPE = "const";
    String JAEGER_SAMPLER_PARAM = "1";
    String JAEGER_OPERATOR_DEPLOYMENT_NAME = "jaeger-operator";
    String JAEGER_AGENT_HOST = JAEGER_INSTANCE_NAME + "-agent";
    String JAEGER_QUERY_SERVICE = JAEGER_INSTANCE_NAME + "-query";
    String JAEGER_COLLECTOR_NAME = JAEGER_INSTANCE_NAME + "-collector";
    String JAEGER_COLLECTOR_OTLP_URL = "http://" + JAEGER_COLLECTOR_NAME + ":4317";

    String CERT_MANAGER_WEBHOOK_DEPLOYMENT = "cert-manager-webhook";
    String CERT_MANAGER_CA_INJECTOR_DEPLOYMENT = "cert-manager-cainjector";
    String CERT_MANAGER_DEPLOYMENT = "cert-manager";
    String CERT_MANAGER_NAMESPACE = "cert-manager";

    String JAEGER_SERVICE_ENV = "JAEGER_SERVICE_NAME";
    String OTEL_SERVICE_ENV = "OTEL_SERVICE_NAME";

    String OPEN_TELEMETRY = "OpenTelemetry";
    String OPEN_TRACING = "OpenTracing";
}
