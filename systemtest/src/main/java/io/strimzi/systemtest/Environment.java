/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Locale;

/**
 * Class which holds environment variables for system tests.
 */
public class Environment {

    private static final Logger LOGGER = LogManager.getLogger(Environment.class);

    /**
     * Specify organization which owns image used in system tests.
     */
    private static final String STRIMZI_ORG_ENV = "DOCKER_ORG";
    /**
     * Specify registry for images used in system tests.
     */
    private static final String STRIMZI_REGISTRY_ENV = "DOCKER_REGISTRY";
    /**
     * Specify image tags used in system tests.
     */
    private static final String STRIMZI_TAG_ENV = "DOCKER_TAG";
    /**
     * Specify test-client image used in system tests.
     */
    private static final String TEST_CLIENT_IMAGE_ENV = "TEST_CLIENT_IMAGE";
    /**
     * Specify kafka bridge image used in system tests.
     */
    private static final String BRIDGE_IMAGE_ENV = "BRIDGE_IMAGE";
    /**
     * Directory for store logs collected during the tests.
     */
    private static final String TEST_LOG_DIR_ENV = "TEST_LOG_DIR";
    /**
     * Kafka version used in images during the system tests.
     */
    private static final String ST_KAFKA_VERSION_ENV = "ST_KAFKA_VERSION";
    /**
     * Log level for cluster operator.
     */
    private static final String STRIMZI_LOG_LEVEL_ENV = "STRIMZI_DEFAULT_LOG_LEVEL";
    /**
     * Cluster domain. It's used for specify URL endpoint of testing clients.
     */
    private static final String KUBERNETES_DOMAIN_ENV = "KUBERNETES_DOMAIN";
    /**
     * Image pull policy env var for Components images (Kafka, Bridge, ...)
     */
    private static final String COMPONENTS_IMAGE_PULL_POLICY_ENV = "COMPONENTS_IMAGE_PULL_POLICY";
    /**
     * Image pull policy env var for Operator images
     */
    private static final String OPERATOR_IMAGE_PULL_POLICY_ENV = "OPERATOR_IMAGE_PULL_POLICY";
    /**
     * CO reconciliation interval.
     */
    private static final String STRIMZI_FULL_RECONCILIATION_INTERVAL_MS_ENV = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";
    /**
     * OLM env variables
     */
    private static final String OLM_OPERATOR_NAME_ENV = "OLM_OPERATOR_NAME";
    private static final String OLM_SOURCE_NAME_ENV = "OLM_SOURCE_NAME";
    private static final String OLM_APP_BUNDLE_PREFIX_ENV = "OLM_APP_BUNDLE_PREFIX";
    private static final String OLM_OPERATOR_VERSION_ENV = "OLM_OPERATOR_VERSION";
    /**
     * Allows network policies
     */
    private static final String DEFAULT_TO_DENY_NETWORK_POLICIES_ENV = "DEFAULT_TO_DENY_NETWORK_POLICIES";
    /**
     * ClusterOperator installation type
     */
    private static final String CLUSTER_OPERATOR_INSTALL_TYPE_ENV = "CLUSTER_OPERATOR_INSTALL_TYPE";

    private static final String SKIP_TEARDOWN_ENV = "SKIP_TEARDOWN";

    private static final String ST_KAFKA_VERSION_DEFAULT = "2.5.0";
    public static final String STRIMZI_ORG_DEFAULT = "strimzi";
    public static final String STRIMZI_TAG_DEFAULT = "latest";
    public static final String STRIMZI_REGISTRY_DEFAULT = "docker.io";
    private static final String TEST_LOG_DIR_DEFAULT = "../systemtest/target/logs/";
    private static final String STRIMZI_LOG_LEVEL_DEFAULT = "DEBUG";
    static final String KUBERNETES_DOMAIN_DEFAULT = ".nip.io";
    public static final String COMPONENTS_IMAGE_PULL_POLICY_ENV_DEFAULT = Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY;
    public static final String OPERATOR_IMAGE_PULL_POLICY_ENV_DEFAULT = Constants.ALWAYS_IMAGE_PULL_POLICY;
    public static final int KAFKA_CLIENTS_DEFAULT_PORT = 4242;
    public static final String OLM_OPERATOR_NAME_DEFAULT = "strimzi";
    public static final String OLM_SOURCE_NAME_DEFAULT = "strimzi-source";
    public static final String OLM_APP_BUNDLE_PREFIX_DEFAULT = "strimzi";
    public static final String OLM_OPERATOR_VERSION_DEFAULT = "v0.18.0";
    private static final String DEFAULT_TO_DENY_NETWORK_POLICIES_DEFAULT = "true";
    private static final String CLUSTER_OPERATOR_INSTALL_TYPE_DEFAULT = "bundle";

    public static final String STRIMZI_ORG = System.getenv().getOrDefault(STRIMZI_ORG_ENV, STRIMZI_ORG_DEFAULT);
    public static final String STRIMZI_TAG = System.getenv().getOrDefault(STRIMZI_TAG_ENV, STRIMZI_TAG_DEFAULT);
    public static final String STRIMZI_REGISTRY = System.getenv().getOrDefault(STRIMZI_REGISTRY_ENV, STRIMZI_REGISTRY_DEFAULT);
    public static final String TEST_LOG_DIR = System.getenv().getOrDefault(TEST_LOG_DIR_ENV, TEST_LOG_DIR_DEFAULT);
    public static final String ST_KAFKA_VERSION = System.getenv().getOrDefault(ST_KAFKA_VERSION_ENV, ST_KAFKA_VERSION_DEFAULT);
    public static final String STRIMZI_LOG_LEVEL = System.getenv().getOrDefault(STRIMZI_LOG_LEVEL_ENV, STRIMZI_LOG_LEVEL_DEFAULT);
    public static final String KUBERNETES_DOMAIN = System.getenv().getOrDefault(KUBERNETES_DOMAIN_ENV, KUBERNETES_DOMAIN_DEFAULT);
    public static final String SKIP_TEARDOWN = System.getenv(SKIP_TEARDOWN_ENV);
    // variables for test-client image
    private static final String TEST_CLIENT_IMAGE_DEFAULT = STRIMZI_REGISTRY + "/" + STRIMZI_ORG + "/test-client:" + STRIMZI_TAG + "-kafka-" + ST_KAFKA_VERSION;
    public static final String TEST_CLIENT_IMAGE = System.getenv().getOrDefault(TEST_CLIENT_IMAGE_ENV, TEST_CLIENT_IMAGE_DEFAULT);
    // variables for kafka bridge image
    private static final String BRIDGE_IMAGE_DEFAULT = "latest-released";
    public static final String BRIDGE_IMAGE = System.getenv().getOrDefault(BRIDGE_IMAGE_ENV, BRIDGE_IMAGE_DEFAULT);
    // Image pull policy variables
    public static final String COMPONENTS_IMAGE_PULL_POLICY = System.getenv().getOrDefault(COMPONENTS_IMAGE_PULL_POLICY_ENV, COMPONENTS_IMAGE_PULL_POLICY_ENV_DEFAULT);
    public static final String OPERATOR_IMAGE_PULL_POLICY = System.getenv().getOrDefault(OPERATOR_IMAGE_PULL_POLICY_ENV, OPERATOR_IMAGE_PULL_POLICY_ENV_DEFAULT);
    // OLM env variables
    public static final String OLM_OPERATOR_NAME = System.getenv().getOrDefault(OLM_OPERATOR_NAME_ENV, OLM_OPERATOR_NAME_DEFAULT);
    public static final String OLM_SOURCE_NAME = System.getenv().getOrDefault(OLM_SOURCE_NAME_ENV, OLM_SOURCE_NAME_DEFAULT);
    public static final String OLM_APP_BUNDLE_PREFIX = System.getenv().getOrDefault(OLM_APP_BUNDLE_PREFIX_ENV, OLM_APP_BUNDLE_PREFIX_DEFAULT);
    public static final String OLM_OPERATOR_VERSION = System.getenv().getOrDefault(OLM_OPERATOR_VERSION_ENV, OLM_OPERATOR_VERSION_DEFAULT);
    // NetworkPolicy variable
    public static final String DEFAULT_TO_DENY_NETWORK_POLICIES = System.getenv().getOrDefault(DEFAULT_TO_DENY_NETWORK_POLICIES_ENV, DEFAULT_TO_DENY_NETWORK_POLICIES_DEFAULT);
    // ClusterOperator installation type variable
    public static final String CLUSTER_OPERATOR_INSTALL_TYPE = System.getenv().getOrDefault(CLUSTER_OPERATOR_INSTALL_TYPE_ENV, CLUSTER_OPERATOR_INSTALL_TYPE_DEFAULT);


    private Environment() { }

    static {
        String debugFormat = "{}: {}";
        LOGGER.info("Used environment variables:");
        LOGGER.info(debugFormat, STRIMZI_ORG_ENV, STRIMZI_ORG);
        LOGGER.info(debugFormat, STRIMZI_TAG_ENV, STRIMZI_TAG);
        LOGGER.info(debugFormat, STRIMZI_REGISTRY_ENV, STRIMZI_REGISTRY);
        LOGGER.info(debugFormat, TEST_CLIENT_IMAGE_ENV, TEST_CLIENT_IMAGE);
        LOGGER.info(debugFormat, BRIDGE_IMAGE_ENV, BRIDGE_IMAGE);
        LOGGER.info(debugFormat, TEST_LOG_DIR_ENV, TEST_LOG_DIR);
        LOGGER.info(debugFormat, ST_KAFKA_VERSION_ENV, ST_KAFKA_VERSION);
        LOGGER.info(debugFormat, STRIMZI_LOG_LEVEL_ENV, STRIMZI_LOG_LEVEL);
        LOGGER.info(debugFormat, KUBERNETES_DOMAIN_ENV, KUBERNETES_DOMAIN);
        LOGGER.info(debugFormat, COMPONENTS_IMAGE_PULL_POLICY_ENV, COMPONENTS_IMAGE_PULL_POLICY);
        LOGGER.info(debugFormat, OPERATOR_IMAGE_PULL_POLICY_ENV, OPERATOR_IMAGE_PULL_POLICY);
        LOGGER.info(debugFormat, OLM_OPERATOR_NAME_ENV, OLM_OPERATOR_NAME);
        LOGGER.info(debugFormat, OLM_APP_BUNDLE_PREFIX_ENV, OLM_APP_BUNDLE_PREFIX);
        LOGGER.info(debugFormat, OLM_OPERATOR_VERSION_ENV, OLM_OPERATOR_VERSION);
        LOGGER.info(debugFormat, DEFAULT_TO_DENY_NETWORK_POLICIES_ENV, DEFAULT_TO_DENY_NETWORK_POLICIES);
        LOGGER.info(debugFormat, CLUSTER_OPERATOR_INSTALL_TYPE_ENV, CLUSTER_OPERATOR_INSTALL_TYPE);
    }

    public static boolean isOlmInstall() {
        return Environment.CLUSTER_OPERATOR_INSTALL_TYPE.toUpperCase(Locale.ENGLISH).equals("OLM");
    }

    public static boolean isHelmInstall() {
        return Environment.CLUSTER_OPERATOR_INSTALL_TYPE.toUpperCase(Locale.ENGLISH).equals("HELM");
    }

    public static boolean useLatestReleasedBridge() {
        return Environment.BRIDGE_IMAGE.equals(Environment.BRIDGE_IMAGE_DEFAULT);
    }
}
