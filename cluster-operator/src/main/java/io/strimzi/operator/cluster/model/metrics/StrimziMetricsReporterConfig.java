/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.metrics;

/**
 * This class holds Strimzi Metrics Reporter and related configuration keys.
 */
public class StrimziMetricsReporterConfig {
    /** KafkaMetricsReporter fully qualified class name. */
    public static final String KAFKA_CLASS = "io.strimzi.kafka.metrics.KafkaPrometheusMetricsReporter";

    /** YammerMetricsReporter fully qualified class name. */
    public static final String YAMMER_CLASS = "io.strimzi.kafka.metrics.YammerPrometheusMetricsReporter";

    /** Enable HTTP listener (true/false). */
    public static final String LISTENER_ENABLE = "prometheus.metrics.reporter.listener.enable";

    /** Set listener endpoint (e.g. http://:8080). */
    public static final String LISTENER = "prometheus.metrics.reporter.listener";

    /** Set a comma separated list of regexes. */
    public static final String ALLOW_LIST = "prometheus.metrics.reporter.allowlist";
}