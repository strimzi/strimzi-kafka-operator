/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.test.TestUtils;

import java.time.Duration;
import java.util.Map;

/**
 * Interface for keep global constants used across system tests.
 */
public interface Constants {
    long TIMEOUT_FOR_RESOURCE_RECOVERY = Duration.ofMinutes(6).toMillis();
    long TIMEOUT_FOR_MIRROR_MAKER_COPY_MESSAGES_BETWEEN_BROKERS = Duration.ofMinutes(7).toMillis();
    long TIMEOUT_FOR_LOG = Duration.ofMinutes(2).toMillis();
    long POLL_INTERVAL_FOR_RESOURCE_READINESS = Duration.ofSeconds(1).toMillis();
    long POLL_INTERVAL_FOR_RESOURCE_DELETION = Duration.ofSeconds(5).toMillis();
    long WAIT_FOR_ROLLING_UPDATE_INTERVAL = Duration.ofSeconds(5).toMillis();

    long TIMEOUT_FOR_SEND_RECEIVE_MSG = Duration.ofSeconds(60).toMillis();
    long TIMEOUT_FOR_CLUSTER_STABLE = Duration.ofMinutes(20).toMillis();

    long TIMEOUT_TEARDOWN = Duration.ofSeconds(10).toMillis();
    long GLOBAL_TIMEOUT = Duration.ofMinutes(5).toMillis();
    long GLOBAL_TIMEOUT_SHORT = Duration.ofMinutes(2).toMillis();
    long GLOBAL_CMD_CLIENT_TIMEOUT = Duration.ofMinutes(5).toMillis();
    long GLOBAL_STATUS_TIMEOUT = Duration.ofMinutes(3).toMillis();
    long GLOBAL_POLL_INTERVAL = Duration.ofSeconds(1).toMillis();
    long GLOBAL_POLL_INTERVAL_MEDIUM = Duration.ofSeconds(10).toMillis();
    long PRODUCER_TIMEOUT = Duration.ofSeconds(25).toMillis();

    long GLOBAL_TRACING_POLL = Duration.ofSeconds(30).toMillis();

    long API_CRUISE_CONTROL_POLL = Duration.ofSeconds(5).toMillis();
    long API_CRUISE_CONTROL_TIMEOUT = Duration.ofMinutes(10).toMillis();
    long GLOBAL_CRUISE_CONTROL_TIMEOUT = Duration.ofMinutes(2).toMillis();

    long OLM_UPGRADE_INSTALL_PLAN_TIMEOUT = Duration.ofMinutes(15).toMillis();
    long OLM_UPGRADE_INSTALL_PLAN_POLL = Duration.ofMinutes(1).toMillis();

    long GLOBAL_CLIENTS_POLL = Duration.ofSeconds(15).toMillis();
    long GLOBAL_CLIENTS_TIMEOUT = Duration.ofMinutes(2).toMillis();
    long GLOBAL_CLIENTS_EXCEPT_ERROR_TIMEOUT = Duration.ofSeconds(10).toMillis();

    long CO_OPERATION_TIMEOUT_DEFAULT = Duration.ofMinutes(5).toMillis();
    long CO_OPERATION_TIMEOUT_SHORT = Duration.ofSeconds(30).toMillis();
    long CO_OPERATION_TIMEOUT_MEDIUM = Duration.ofMinutes(2).toMillis();
    long RECONCILIATION_INTERVAL = Duration.ofSeconds(30).toMillis();
    long SAFETY_RECONCILIATION_INTERVAL = (RECONCILIATION_INTERVAL + Duration.ofSeconds(10).toMillis()) * 2;
    long LOGGING_RELOADING_INTERVAL = Duration.ofSeconds(30).toMillis();
    long CC_LOG_CONFIG_RELOAD = Duration.ofSeconds(5).toMillis();

    // Keycloak
    long KEYCLOAK_DEPLOYMENT_POLL = Duration.ofSeconds(5).toMillis();
    long KEYCLOAK_DEPLOYMENT_TIMEOUT = Duration.ofMinutes(10).toMillis();

    // stability count ensures that after some reconciliation we have some additional time
    int GLOBAL_STABILITY_OFFSET_COUNT = 20;
    // it is replacement instead of checking logs for reconciliation using dynamic waiting on some change for some period of time
    int GLOBAL_RECONCILIATION_COUNT = (int) ((RECONCILIATION_INTERVAL / GLOBAL_POLL_INTERVAL) + GLOBAL_STABILITY_OFFSET_COUNT);

    long THROTTLING_EXCEPTION_TIMEOUT = Duration.ofMinutes(10).toMillis();

    // sometimes each call `curl -X GET http://localhost:8083/connectors` could take in maximum 13s, and we do 50 calls; meaning (13s * 50)/60 ~= 11m
    long KAFKA_CONNECTOR_STABILITY_TIMEOUT = Duration.ofMinutes(12).toMillis();

    // Jaeger
    long JAEGER_DEPLOYMENT_TIMEOUT = Duration.ofMinutes(4).toMillis();
    long JAEGER_DEPLOYMENT_POLL = Duration.ofMinutes(1).toMillis();

    /**
     * Constants for KafkaConnect EchoSink plugin
      */
    String ECHO_SINK_CONNECTOR_NAME = "echo-sink-connector";
    String ECHO_SINK_CLASS_NAME = "cz.scholz.kafka.connect.echosink.EchoSinkConnector";
    String ECHO_SINK_TGZ_URL = "https://github.com/scholzj/echo-sink/archive/1.3.1.tar.gz";
    String ECHO_SINK_TGZ_CHECKSUM = "6b360470e0a9aa92977be4e669a99d324149ed1544db91a527e6af5f25e9b01fd53e18eeae675c5edc7b8237aeeba9265bf999d77bb1e16df3e4263b3a5003b3";
    String ECHO_SINK_JAR_URL = "https://github.com/scholzj/echo-sink/releases/download/1.3.1/echo-sink-1.3.1.jar";
    String ECHO_SINK_JAR_CHECKSUM = "1d59ede165c0d547e3217d20fd40d7f67ed820c78fc9b5551a3cea53c5928479dc8f5ddf8806d1775e9080bac6a59d044456402c375ae5393f67b96171df7caf";
    String ECHO_SINK_FILE_NAME = "echo-sink-test.jar";
    String ECHO_SINK_JAR_WRONG_CHECKSUM = "f1f167902325062efc8c755647bc1b782b2b067a87a6e507ff7a3f6205803220";

    /**
     * Scraper pod labels
     */
    String SCRAPER_LABEL_KEY = "user-test-app";
    String SCRAPER_LABEL_VALUE = "scraper";

    String SCRAPER_NAME = "scraper";

    /**
     * Constants for Kafka clients labels
     */
    String KAFKA_CLIENTS_LABEL_KEY = "user-test-app";
    String KAFKA_ADMIN_CLIENT_LABEL_KEY = "user-test-admin-app";
    String KAFKA_CLIENTS_LABEL_VALUE = "kafka-clients";
    String KAFKA_ADMIN_CLIENT_LABEL_VALUE = "kafka-clients";
    String KAFKA_BRIDGE_CLIENTS_LABEL_VALUE = "kafka-clients";

    String STRIMZI_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    String ALWAYS_IMAGE_PULL_POLICY = "Always";
    String IF_NOT_PRESENT_IMAGE_PULL_POLICY = "IfNotPresent";

    /**
     * Drain Cleaner related constants
     */
    String DRAIN_CLEANER_DEPLOYMENT_NAME = "strimzi-drain-cleaner";
    String DRAIN_CLEANER_NAMESPACE = "strimzi-drain-cleaner";

    /**
     * Constants for specific ports
     */
    int COMPONENTS_METRICS_PORT = 9404;
    int CLUSTER_OPERATOR_METRICS_PORT = 8080;
    int USER_OPERATOR_METRICS_PORT = 8081;
    int TOPIC_OPERATOR_METRICS_PORT = 8080;
    int KAFKA_BRIDGE_METRICS_PORT = 8080;
    int JMX_PORT = 9999;

    String DEPLOYMENT = "Deployment";
    String DEPLOYMENT_TYPE = "deployment-type";
    String SERVICE = "Service";
    String CONFIG_MAP = "ConfigMap";
    String LEASE = "Lease";
    String SERVICE_ACCOUNT = "ServiceAccount";
    String CLUSTER_ROLE = "ClusterRole";
    String CLUSTER_ROLE_BINDING = "ClusterRoleBinding";
    String CUSTOM_RESOURCE_DEFINITION = "CustomResourceDefinition";
    String CUSTOM_RESOURCE_DEFINITION_SHORT = "Crd";
    String ROLE_BINDING = "RoleBinding";
    String ROLE = "Role";
    String DEPLOYMENT_CONFIG = "DeploymentConfig";
    String SECRET = "Secret";
    String KAFKA_EXPORTER_DEPLOYMENT = "KafkaWithExporter";
    String KAFKA_CRUISE_CONTROL_DEPLOYMENT = "KafkaWithCruiseControl";
    String STATEFUL_SET = "StatefulSet";
    String POD = "Pod";
    String NETWORK_POLICY = "NetworkPolicy";
    String JOB = "Job";
    String VALIDATION_WEBHOOK_CONFIG = "ValidatingWebhookConfiguration";
    String REPLICA_SET = "ReplicaSet";
    String SUBSCRIPTION = "Subscription";
    String OPERATOR_GROUP = "OperatorGroup";

    /**
     * Kafka Bridge JSON encoding with JSON embedded format
     */
    String KAFKA_BRIDGE_JSON_JSON = "application/vnd.kafka.json.v2+json";
    String DEFAULT_SINK_FILE_PATH = "/tmp/test-file-sink.txt";

    int HTTP_BRIDGE_DEFAULT_PORT = 8080;
    int HTTPS_KEYCLOAK_DEFAULT_NODE_PORT = 32481;

    /**
     * Basic paths to examples
     */
    String PATH_TO_PACKAGING_EXAMPLES = TestUtils.USER_PATH + "/../packaging/examples";
    String PATH_TO_PACKAGING_INSTALL_FILES = TestUtils.USER_PATH + "/../packaging/install";

    /**
     * File paths for metrics YAMLs
     */
    String PATH_TO_KAFKA_METRICS_CONFIG = PATH_TO_PACKAGING_EXAMPLES + "/metrics/kafka-metrics.yaml";

    String METRICS_CONFIG_YAML_NAME = "metrics-config.yml";
    String METRICS_CONFIG_JSON_NAME = "metrics-config.json";

    String PATH_TO_KAFKA_CONNECT_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/connect/kafka-connect.yaml";
    String PATH_TO_KAFKA_CONNECT_METRICS_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/metrics/kafka-connect-metrics.yaml";
    String PATH_TO_KAFKA_BRIDGE_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/bridge/kafka-bridge.yaml";
    String PATH_TO_KAFKA_REBALANCE_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/cruise-control/kafka-rebalance-full.yaml";
    String PATH_TO_KAFKA_CRUISE_CONTROL_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/cruise-control/kafka-cruise-control.yaml";
    String PATH_TO_KAFKA_EPHEMERAL_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/kafka/kafka-ephemeral.yaml";
    String PATH_TO_KAFKA_PERSISTENT_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/kafka/kafka-persistent.yaml";
    String PATH_TO_KAFKA_CRUISE_CONTROL_METRICS_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/metrics/kafka-cruise-control-metrics.yaml";
    String PATH_TO_KAFKA_TOPIC_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml";
    String PATH_TO_KAFKA_CONNECTOR_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/connect/source-connector.yaml";
    String PATH_TO_KAFKA_MIRROR_MAKER_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/mirror-maker/kafka-mirror-maker.yaml";
    String PATH_TO_KAFKA_MIRROR_MAKER_2_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/mirror-maker/kafka-mirror-maker-2.yaml";
    String PATH_TO_KAFKA_MIRROR_MAKER_2_METRICS_CONFIG = Constants.PATH_TO_PACKAGING_EXAMPLES + "/metrics/kafka-mirror-maker-2-metrics.yaml";

    /**
     * Feature gate related constants
     */
    String USE_STRIMZI_POD_SET = "+UseStrimziPodSets";
    String USE_STRIMZI_STATEFULSETS = "-UseStrimziPodSets";
    String USE_KRAFT_MODE = "+UseKRaft";

    /**
     * Default value which allows execution of tests with any tags
     */
    String DEFAULT_TAG = "all";

    /**
     * Tag for acceptance tests, which can be triggered manually for each push/pr/merge on Azure
     */
    String ACCEPTANCE = "acceptance";

    /**
     * Tag for regression tests which are stable.
     */
    String REGRESSION = "regression";

    /**
     * Tag for upgrade tests.
     */
    String UPGRADE = "upgrade";

    /**
     * Tag for olm upgrade tests
     */
    String OLM_UPGRADE = "olmupgrade";

    /**
     * Tag for smoke tests
     */
    String SMOKE = "smoke";

    /**
     * Tag for Kafka smoke tests
     */
    String KAFKA_SMOKE = "kafkasmoke";

    /**
     * Tag for sanity tests
     */
    String SANITY = "sanity";

    /**
     * Tag for tests, which results are not 100% reliable on all testing environments.
     */
    String FLAKY = "flaky";

    /**
     * Tag for scalability tests
     */
    String SCALABILITY = "scalability";

    /**
     * Tag for tests containing scaling of particular component (scaling up and down)
     */
    String COMPONENT_SCALING = "componentscaling";

    /**
     * Tag for tests, which are working only on specific environment and we usually don't want to execute them on all environments.
     */
    String SPECIFIC = "specific";

    /**
     * Tag for tests, which are using NodePort.
     */
    String NODEPORT_SUPPORTED = "nodeport";

    /**
     * Tag for tests, which are using LoadBalancer.
     */
    String LOADBALANCER_SUPPORTED = "loadbalancer";

    /**
     * Tag for tests, which are using NetworkPolicies.
     */
    String NETWORKPOLICIES_SUPPORTED = "networkpolicies";

    /**
     * Tag for Prometheus tests
     */
    String PROMETHEUS = "prometheus";

    /**
     * Tag for Tracing tests
     */
    String TRACING = "tracing";

    /**
     * Tag for Helm tests
     */
    String HELM = "helm";

    /**
     * Tag for oauth tests
     */
    String OAUTH = "oauth";

    /**
     * Tag for recovery tests
     */
    String RECOVERY = "recovery";

    /**
     * Tag for tests which deploys KafkaConnector resource
     */
    String CONNECTOR_OPERATOR = "connectoroperator";

    /**
     * Tag for tests which deploys KafkaConnect resource
     */
    String CONNECT = "connect";

    /**
     * Tag for tests which deploys KafkaMirrorMaker resource
     */
    String MIRROR_MAKER = "mirrormaker";

    /**
     * Tag for tests which deploys KafkaMirrorMaker2 resource
     */
    String MIRROR_MAKER2 = "mirrormaker2";

    /**
     * Tag for tests which deploys any of KafkaConnect, KafkaConnector, KafkaMirrorMaker2
     */
    String CONNECT_COMPONENTS = "connectcomponents";

    /**
     * Tag for tests which deploys KafkaBridge resource
     */
    String BRIDGE = "bridge";

    /**
     * Tag for tests which use internal Kafka clients (used clients in cluster)
     */
    String INTERNAL_CLIENTS_USED = "internalclients";

    /**
     * Tag for tests which use external Kafka clients (called from test code)
     */
    String EXTERNAL_CLIENTS_USED = "externalclients";

    /**
     * Tag for tests where metrics are used
     */
    String METRICS = "metrics";

    /**
     * Tag for tests where cruise control used
     */
    String CRUISE_CONTROL = "cruisecontrol";

    /**
     * Tag for tests where mainly dynamic configuration is used
     */
    String DYNAMIC_CONFIGURATION = "dynamicconfiguration";

    /**
     * Tag for tests which contains rolling update of resource
     */
    String ROLLING_UPDATE = "rollingupdate";

    /**
     * Tag for tests, for Pod Security profiles set to restricted
     */
    String POD_SECURITY_PROFILES_RESTRICTED = "podsecurityprofiles";

    /**
     * Tag for tests where OLM is used for deploying CO
     */
    String OLM = "olm";

    String ISOLATED_TEST = "isolatedtest";
    String PARALLEL_TEST = "paralleltest";
    /**
     * Tag for tests which executing in parallel namespaces
     */
    String PARALLEL_NAMESPACE = "parallelnamespace";
    // label for test case used for parallel execution of test suites
    String PARALLEL_SUITE = "parallelsuite";
    // label for test case used for isolation of test suites
    String ISOLATED_SUITE = "isolatedsuite";

    /**
     * Constants for filtering and matching our test suites names
     */
    String ISOLATED = "Isolated";
    String ST = "ST";

    String TEST_CASE_NAME_LABEL = "test.case";
    String TEST_SUITE_NAME_LABEL = "test.suite";

    /**
     * Cruise Control related parameters
     */
    String CRUISE_CONTROL_NAME = "Cruise Control";
    String CRUISE_CONTROL_CONTAINER_NAME = "cruise-control";
    String CRUISE_CONTROL_CONFIGURATION_ENV = "CRUISE_CONTROL_CONFIGURATION";
    String CRUISE_CONTROL_CAPACITY_FILE_PATH = "/tmp/capacity.json";
    String CRUISE_CONTROL_CONFIGURATION_FILE_PATH = "/tmp/cruisecontrol.properties";
    String CRUISE_CONTROL_LOG_FILE_PATH = "/opt/cruise-control/custom-config/log4j2.properties";

    /**
     * Default listeners names
     */
    String PLAIN_LISTENER_DEFAULT_NAME = "plain";
    String TLS_LISTENER_DEFAULT_NAME = "tls";
    String EXTERNAL_LISTENER_DEFAULT_NAME = "external";
    String CLUSTER_IP_LISTENER_DEFAULT_NAME = "clusterip";

    /**
     * Loadbalancer finalizer config
     */
    String LOAD_BALANCER_CLEANUP = "service.kubernetes.io/load-balancer-cleanup";

    // main namespace for Cluster Operator deployment
    String INFRA_NAMESPACE = "infra-namespace";
    String METRICS_SECOND_NAMESPACE = "second-metrics-cluster-test";

    /**
     * Auxiliary variables for storing data across our tests
     */
    String NAMESPACE_KEY = "NAMESPACE_NAME";
    String PREPARE_OPERATOR_ENV_KEY = "PREPARE_OPERATOR_ENV";

    // Count of test messages that needs to be sent during the test
    int MESSAGE_COUNT = 100;

    /**
     * Auxiliary variable for cluster operator deployment
     */
    String WATCH_ALL_NAMESPACES = "*";

    String CLUSTER_KEY = "CLUSTER_NAME";
    String TARGET_CLUSTER_KEY = "TARGET_CLUSTER_NAME";
    String TOPIC_KEY = "TOPIC_NAME";
    String TARGET_TOPIC_KEY = "TARGET_TOPIC_NAME";
    String STREAM_TOPIC_KEY = "STREAM_TOPIC_NAME";
    String SCRAPER_KEY = "SCRAPER_NAME";
    String PRODUCER_KEY = "PRODUCER_NAME";
    String CONSUMER_KEY = "CONSUMER_NAME";
    String ADMIN_KEY = "ADMIN_NAME";
    String USER_NAME_KEY = "USER_NAME";
    String ENTITY_OPERATOR_NAME_KEY = "ENTITY_OPERATOR_NAME";
    String KAFKA_STATEFULSET_NAME_KEY = "KAFKA_STATEFULSET_NAME";
    String ZOOKEEPER_STATEFULSET_NAME_KEY = "ZOOKEEPER_STATEFULSET_NAME";
    String SCRAPER_POD_KEY = "SCRAPER_POD_NAME";
    String KAFKA_TRACING_CLIENT_KEY = "KAFKA_TRACING_CLIENT";
    String KAFKA_SELECTOR = "KAFKA_SELECTOR";
    String ZOOKEEPER_SELECTOR = "ZOOKEEPER_SELECTOR";
    String MESSAGE_COUNT_KEY = "MESSAGE_COUNT";

    /**
     * Lease related resources - ClusterRole, Role, RoleBinding
     */
    String PATH_TO_LEASE_CLUSTER_ROLE = PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-ClusterRole-strimzi-cluster-operator-role.yaml";
    // Path after change of ClusterRole -> Role in our SetupClusterOperator class
    String PATH_TO_LEASE_ROLE = PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-Role-strimzi-cluster-operator-role.yaml";
    String PATH_TO_LEASE_ROLE_BINDING = PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-RoleBinding-strimzi-cluster-operator.yaml";
    Map<String, String> LEASE_FILES_AND_RESOURCES = Map.of(
        CLUSTER_ROLE, PATH_TO_LEASE_CLUSTER_ROLE,
        ROLE, PATH_TO_LEASE_ROLE,
        ROLE_BINDING, PATH_TO_LEASE_ROLE_BINDING
    );
}
