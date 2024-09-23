/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

/**
 * Defines constants used as tags in the tests. These tags categorize and filter
 * tests based on their purpose, stability, and environment, enabling selective execution of tests
 * for scenarios like regression, scalability, upgrades, and performance.
 */
public interface Tags {

    /**
     * Feature gate related constants
     */
    // No Feature gates kept for STs at this moment

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
     * Tag for KRaft to KRaft tests.
     */
    String KRAFT_UPGRADE = "kraftupgrade";

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
     * Tag for tests which use external Kafka clients (called from test code)
     */
    String EXTERNAL_CLIENTS_USED = "externalclients";

    /**
     * Tag for tests where metrics are used
     */
    String METRICS = "metrics";

    /**
     * Tag for tests where CruiseControl is used
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

    /**
     * Tag for tests using Openshift Route
     */
    String ROUTE = "route";

    /**
     * Tag for tests that focus on migration from ZK to KRaft
     */
    String MIGRATION = "migration";

    /**
     * Tag for tests that focus on performance
     */
    String PERFORMANCE = "performance";

    /**
     * Tag for tests that uses Strimzi quotas plugin
     */
    String QUOTAS_PLUGIN = "quotasplugin";

    /**
     * Tag for tests that covers Tiered Storage integration
     */
    String TIERED_STORAGE = "tieredstorage";
    String ISOLATED_TEST = "isolatedtest";
    String PARALLEL_TEST = "paralleltest";
    /**
     * Tag for tests which executing in parallel namespaces
     */
    String PARALLEL_NAMESPACE = "parallelnamespace";

    /**
     * Performance specific related tags
     */
    String CAPACITY = "capacity";
}
