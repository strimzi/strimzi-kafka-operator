/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model.cruisecontrol;

/**
 * Enum class holding some Cruise Control configuration options
 */
public enum CruiseControlConfigurationParameters {
    /**
     * Maximum number of concurrent partitions movements per broker
     */
    CONCURRENT_PARTITION_MOVEMENTS("num.concurrent.partition.movements.per.broker"),

    /**
     * Maximum number of concurrent intra-broker partition movements
     */
    CONCURRENT_INTRA_PARTITION_MOVEMENTS("num.concurrent.intra.broker.partition.movements"),

    /**
     * Maximum number of concurrent leader movements
     */
    CONCURRENT_LEADER_MOVEMENTS("num.concurrent.leader.movements"),

    /**
     * Default replication throttle
     */
    REPLICATION_THROTTLE("default.replication.throttle"),

    /**
     * Size of the partition aggregation window
     */
    PARTITION_METRICS_WINDOW_MS_CONFIG_KEY("partition.metrics.window.ms"),

    /**
     * Maximum number of partition windows which will be kept by the load monitor
     */
    PARTITION_METRICS_WINDOW_NUM_CONFIG_KEY("num.partition.metrics.windows"),

    /**
     * Maximum number of windows which will be kept by the load monitor
     */
    BROKER_METRICS_WINDOW_MS_CONFIG_KEY("broker.metrics.window.ms"),

    /**
     * Maximum number of windows which will be kept by the load monitor
     */
    BROKER_METRICS_WINDOW_NUM_CONFIG_KEY("num.broker.metrics.windows"),

    /**
     * Maximum time for storing the completed user tasks
     */
    COMPLETED_USER_TASK_RETENTION_MS_CONFIG_KEY("completed.user.task.retention.time.ms"),

    /**
     * Enables the webserver security
     */
    WEBSERVER_SECURITY_ENABLE("webserver.security.enable"),

    /**
     * Authentication credentials file
     */
    WEBSERVER_AUTH_CREDENTIALS_FILE("webserver.auth.credentials.file"),

    /**
     * Enables TLS in the webserver
     */
    WEBSERVER_SSL_ENABLE("webserver.ssl.enable"),

    /**
     * Topic for partition metric samples
     */
    PARTITION_METRIC_TOPIC_NAME("partition.metric.sample.store.topic"),

    /**
     * Topic for broker metric samples
     */
    BROKER_METRIC_TOPIC_NAME("broker.metric.sample.store.topic"),

    /**
     * Replication factor of Kafka sample store topics
     */
    SAMPLE_STORE_TOPIC_REPLICATION_FACTOR("sample.store.topic.replication.factor"),

    /**
     * Metrics reporter topic
     */
    METRIC_REPORTER_TOPIC_NAME("metric.reporter.topic"),

    /**
     * Capacity config file
     */
    CAPACITY_CONFIG_FILE("capacity.config.file"),

    // Metrics reporter configurations
    /**
     * Metrics reporter bootstrap server
     */
    METRICS_REPORTER_BOOTSTRAP_SERVERS("cruise.control.metrics.reporter.bootstrap.servers"),

    /**
     * Metrics reporter Kubernetes mode
     */
    METRICS_REPORTER_KUBERNETES_MODE("cruise.control.metrics.reporter.kubernetes.mode"),

    /**
     * Metrics reporter security protocol
     */
    METRICS_REPORTER_SECURITY_PROTOCOL("cruise.control.metrics.reporter.security.protocol"),

    /**
     * Metrics reporter TLS hostname verification algorithm
     */
    METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO("cruise.control.metrics.reporter.ssl.endpoint.identification.algorithm"),

    /**
     * Metrics reporter keystore type
     */
    METRICS_REPORTER_SSL_KEYSTORE_TYPE("cruise.control.metrics.reporter.ssl.keystore.type"),

    /**
     * Metrics reporter keystore location
     */
    METRICS_REPORTER_SSL_KEYSTORE_LOCATION("cruise.control.metrics.reporter.ssl.keystore.location"),

    /**
     * Metrics reporter keystore password
     */
    METRICS_REPORTER_SSL_KEYSTORE_PASSWORD("cruise.control.metrics.reporter.ssl.keystore.password"),

    /**
     * Metrics reporter truststore type
     */
    METRICS_REPORTER_SSL_TRUSTSTORE_TYPE("cruise.control.metrics.reporter.ssl.truststore.type"),

    /**
     * Metrics reporter truststore location
     */
    METRICS_REPORTER_SSL_TRUSTSTORE_LOCATION("cruise.control.metrics.reporter.ssl.truststore.location"),

    /**
     * Metrics reporter truststore password
     */
    METRICS_REPORTER_SSL_TRUSTSTORE_PASSWORD("cruise.control.metrics.reporter.ssl.truststore.password"),

    // Metrics topic configurations
    /**
     * Name of the Cruise Control metrics topic
     */
    METRICS_TOPIC_NAME("cruise.control.metrics.topic"),

    /**
     * Should the Cruise Control topic be auto.created
     */
    METRICS_TOPIC_AUTO_CREATE("cruise.control.metrics.topic.auto.create"),

    /**
     * Number of partitions in the Cruise Control metrics topic
     */
    METRICS_TOPIC_NUM_PARTITIONS("cruise.control.metrics.topic.num.partitions"),

    /**
     * Replication factor of the Cruise Control metrics topic
     */
    METRICS_TOPIC_REPLICATION_FACTOR("cruise.control.metrics.topic.replication.factor"),

    /**
     * Number of minimal in-sync replicas in Cruise Control metrics topic
     */
    METRICS_TOPIC_MIN_ISR("cruise.control.metrics.topic.min.insync.replicas"),

    // Goals String lists
    /**
     * Goals configuration key
     */
    GOALS_CONFIG_KEY("goals"),

    /**
     * Default goals configuration key
     */
    DEFAULT_GOALS_CONFIG_KEY("default.goals"),

    /**
     * Hard goals configuration key
     */
    HARD_GOALS_CONFIG_KEY("hard.goals"),

    /**
     * Self-healing goals configuration key
     */
    SELF_HEALING_CONFIG_KEY("self.healing.goals"),

    /**
     * Anomaly detection goals configuration key
     */
    ANOMALY_DETECTION_CONFIG_KEY("anomaly.detection.goals");

    // Defaults
    /**
     * Default value for enabling webserver
     */
    public static final boolean DEFAULT_WEBSERVER_SECURITY_ENABLED = true;

    /**
     * Default value for enabling webserver security
     */
    public static final boolean DEFAULT_WEBSERVER_SSL_ENABLED = true;

    /**
     * Default topic name for the partition samples
     */
    public static final String DEFAULT_PARTITION_METRIC_TOPIC_NAME = "strimzi.cruisecontrol.partitionmetricsamples";

    /**
     * Default topic name for the broker samples
     */
    public static final String DEFAULT_BROKER_METRIC_TOPIC_NAME = "strimzi.cruisecontrol.modeltrainingsamples";

    /**
     * Default topic name for the metrics reporter
     */
    public static final String DEFAULT_METRIC_REPORTER_TOPIC_NAME = "strimzi.cruisecontrol.metrics";

    private final String value;

    /**
     * Constructs the Enum from a String value
     *
     * @param value     Value which will be converted to Enum
     */
    CruiseControlConfigurationParameters(String value) {
        this.value = value;
    }

    /**
     * @return String value of the Enum
     */
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
