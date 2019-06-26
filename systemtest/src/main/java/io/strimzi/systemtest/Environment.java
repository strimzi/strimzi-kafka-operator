/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
     * CO reconciliation interval.
     */
    private static final String STRIMZI_FULL_RECONCILIATION_INTERVAL_MS_ENV = "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS";

    private static final String SKIP_TEARDOWN_ENV = "SKIP_TEARDOWN";

    private static final String ST_KAFKA_VERSION_DEFAULT = "2.2.1";
    public static final String STRIMZI_ORG_DEFAULT = "strimzi";
    public static final String STRIMZI_TAG_DEFAULT = "latest";
    public static final String STRIMZI_REGISTRY_DEFAULT = "docker.io";
    private static final String TEST_LOG_DIR_DEFAULT = "../systemtest/target/logs/";
    private static final String STRIMZI_LOG_LEVEL_DEFAULT = "DEBUG";
    static final String KUBERNETES_DOMAIN_DEFAULT = ".nip.io";
    private static final String STRIMZI_FULL_RECONCILIATION_INTERVAL_MS_DEFAULT = "30000";
    static final int INGRESS_DEFAULT_PORT = 4242;

    public static final String STRIMZI_ORG = System.getenv().getOrDefault(STRIMZI_ORG_ENV, STRIMZI_ORG_DEFAULT);
    public static final String STRIMZI_TAG = System.getenv().getOrDefault(STRIMZI_TAG_ENV, STRIMZI_TAG_DEFAULT);
    public static final String STRIMZI_REGISTRY = System.getenv().getOrDefault(STRIMZI_REGISTRY_ENV, STRIMZI_REGISTRY_DEFAULT);
    static final String TEST_LOG_DIR = System.getenv().getOrDefault(TEST_LOG_DIR_ENV, TEST_LOG_DIR_DEFAULT);
    static final String ST_KAFKA_VERSION = System.getenv().getOrDefault(ST_KAFKA_VERSION_ENV, ST_KAFKA_VERSION_DEFAULT);
    static final String STRIMZI_LOG_LEVEL = System.getenv().getOrDefault(STRIMZI_LOG_LEVEL_ENV, STRIMZI_LOG_LEVEL_DEFAULT);
    static final String KUBERNETES_DOMAIN = System.getenv().getOrDefault(KUBERNETES_DOMAIN_ENV, KUBERNETES_DOMAIN_DEFAULT);
    static final String STRIMZI_FULL_RECONCILIATION_INTERVAL_MS = System.getenv().getOrDefault(STRIMZI_FULL_RECONCILIATION_INTERVAL_MS_ENV, STRIMZI_FULL_RECONCILIATION_INTERVAL_MS_DEFAULT);
    static final String SKIP_TEARDOWN = System.getenv(SKIP_TEARDOWN_ENV);
    // variables for test-client image
    private static final String TEST_CLIENT_IMAGE_DEFAULT = STRIMZI_REGISTRY + "/" + STRIMZI_ORG + "/test-client:" + STRIMZI_TAG + "-kafka-" + ST_KAFKA_VERSION;
    public static final String TEST_CLIENT_IMAGE = System.getenv().getOrDefault(TEST_CLIENT_IMAGE_ENV, TEST_CLIENT_IMAGE_DEFAULT);

    private Environment() {
    }

    static {
        String debugFormat = "{}:{}";
        LOGGER.info("Used environment variables:");
        LOGGER.info(debugFormat, STRIMZI_ORG_ENV, STRIMZI_ORG);
        LOGGER.info(debugFormat, STRIMZI_TAG_ENV, STRIMZI_TAG);
        LOGGER.info(debugFormat, STRIMZI_REGISTRY_ENV, STRIMZI_REGISTRY);
        LOGGER.info(debugFormat, TEST_CLIENT_IMAGE_ENV, TEST_CLIENT_IMAGE);
        LOGGER.info(debugFormat, TEST_LOG_DIR_ENV, TEST_LOG_DIR);
        LOGGER.info(debugFormat, ST_KAFKA_VERSION_ENV, ST_KAFKA_VERSION);
        LOGGER.info(debugFormat, STRIMZI_LOG_LEVEL_ENV, STRIMZI_LOG_LEVEL);
        LOGGER.info(debugFormat, KUBERNETES_DOMAIN_ENV, KUBERNETES_DOMAIN);
    }
}
