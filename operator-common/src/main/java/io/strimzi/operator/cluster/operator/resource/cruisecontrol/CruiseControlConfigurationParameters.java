/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.cruisecontrol;

public enum CruiseControlConfigurationParameters {
    CONCURRENT_PARTITION_MOVEMENTS("num.concurrent.partition.movements.per.broker"),
    CONCURRENT_INTRA_PARTITION_MOVEMENTS("num.concurrent.intra.broker.partition.movements"),
    CONCURRENT_LEADER_MOVEMENTS("num.concurrent.leader.movements"),
    REPLICATION_THROTTLE("default.replication.throttle"),
    PARTITION_METRICS_WINDOW_MS_CONFIG_KEY("partition.metrics.window.ms"),
    PARTITION_METRICS_WINDOW_NUM_CONFIG_KEY("num.partition.metrics.windows"),
    BROKER_METRICS_WINDOW_MS_CONFIG_KEY("broker.metrics.window.ms"),
    BROKER_METRICS_WINDOW_NUM_CONFIG_KEY("num.broker.metrics.windows"),
    COMPLETED_USER_TASK_RETENTION_MS_CONFIG_KEY("completed.user.task.retention.time.ms"),
    WEBSERVER_SECURITY_ENABLE("webserver.security.enable"),
    WEBSERVER_AUTH_CREDENTIALS_FILE("webserver.auth.credentials.file"),
    WEBSERVER_SSL_ENABLE("webserver.ssl.enable"),
    PARTITION_METRIC_TOPIC_NAME("partition.metric.sample.store.topic"),
    BROKER_METRIC_TOPIC_NAME("broker.metric.sample.store.topic"),
    METRIC_REPORTER_TOPIC_NAME("metric.reporter.topic"),

    // Metrics reporter configurations
    METRICS_REPORTER_BOOTSTRAP_SERVERS("cruise.control.metrics.reporter.bootstrap.servers"),
    METRICS_REPORTER_KUBERNETES_MODE("cruise.control.metrics.reporter.kubernetes.mode"),
    METRICS_REPORTER_SECURITY_PROTOCOL("cruise.control.metrics.reporter.security.protocol"),
    METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO("cruise.control.metrics.reporter.ssl.endpoint.identification.algorithm"),
    METRICS_REPORTER_SSL_KEYSTORE_TYPE("cruise.control.metrics.reporter.ssl.keystore.type"),
    METRICS_REPORTER_SSL_KEYSTORE_LOCATION("cruise.control.metrics.reporter.ssl.keystore.location"),
    METRICS_REPORTER_SSL_KEYSTORE_PASSWORD("cruise.control.metrics.reporter.ssl.keystore.password"),
    METRICS_REPORTER_SSL_TRUSTSTORE_TYPE("cruise.control.metrics.reporter.ssl.truststore.type"),
    METRICS_REPORTER_SSL_TRUSTSTORE_LOCATION("cruise.control.metrics.reporter.ssl.truststore.location"),
    METRICS_REPORTER_SSL_TRUSTSTORE_PASSWORD("cruise.control.metrics.reporter.ssl.truststore.password"),

    // Metrics topic configurations
    METRICS_TOPIC_NAME("cruise.control.metrics.topic"),
    METRICS_TOPIC_AUTO_CREATE("cruise.control.metrics.topic.auto.create"),
    METRICS_TOPIC_NUM_PARTITIONS("cruise.control.metrics.topic.num.partitions"),
    METRICS_TOPIC_REPLICATION_FACTOR("cruise.control.metrics.topic.replication.factor"),
    METRICS_TOPIC_MIN_ISR("cruise.control.metrics.topic.min.insync.replicas"),

    // Goals String lists
    GOALS_CONFIG_KEY("goals"),
    DEFAULT_GOALS_CONFIG_KEY("default.goals"),
    HARD_GOALS_CONFIG_KEY("hard.goals"),
    SELF_HEALING_CONFIG_KEY("self.healing.goals"),
    ANOMALY_DETECTION_CONFIG_KEY("anomaly.detection.goals");

    // Defaults
    public static final boolean DEFAULT_WEBSERVER_SECURITY_ENABLED = true;
    public static final boolean DEFAULT_WEBSERVER_SSL_ENABLED = true;
    public static final String DEFAULT_PARTITION_METRIC_TOPIC_NAME = "strimzi.cruisecontrol.partitionmetricsamples";
    public static final String DEFAULT_BROKER_METRIC_TOPIC_NAME = "strimzi.cruisecontrol.modeltrainingsamples";
    public static final String DEFAULT_METRIC_REPORTER_TOPIC_NAME = "strimzi.cruisecontrol.metrics";

    private final String value;

    CruiseControlConfigurationParameters(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
