/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

/**
 * This class holds Strimzi Metrics Reporter and related configuration keys.
 */
public class StrimziMetricsReporterConfig {
    /** ClientMetricsReporter fully qualified class name (for Kafka clients). */
    public static final String CLIENT_CLASS = "io.strimzi.kafka.metrics.prometheus.ClientMetricsReporter";

    /** ServerKafkaMetricsReporter fully qualified class name (for Kafka servers). */
    public static final String SERVER_KAFKA_CLASS = "io.strimzi.kafka.metrics.prometheus.ServerKafkaMetricsReporter";

    /** ServerYammerMetricsReporter fully qualified class name (for Kafka servers). */
    public static final String SERVER_YAMMER_CLASS = "io.strimzi.kafka.metrics.prometheus.ServerYammerMetricsReporter";

    /** Enable HTTP listener (true/false). */
    public static final String LISTENER_ENABLE = "prometheus.metrics.reporter.listener.enable";

    /** Set listener endpoint (e.g. http://:8080). */
    public static final String LISTENER = "prometheus.metrics.reporter.listener";

    /** Set a comma separated list of regexes. */
    public static final String ALLOW_LIST = "prometheus.metrics.reporter.allowlist";

    private StrimziMetricsReporterConfig() { }
}