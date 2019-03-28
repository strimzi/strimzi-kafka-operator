/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Environment {
    private static final Logger LOGGER = LogManager.getLogger(Environment.class);
    private static Environment instance;

    public static final String STRIMZI_ORG_ENV = "STRIMZI_ORG";
    public static final String STRIMZI_TAG_ENV = "STRIMZI_TAG";
    public static final String TEST_LOG_DIR_ENV = "TEST_LOG_DIR";
    public static final String ST_KAFKA_VERSION_ENV = "ST_KAFKA_VERSION";
    public static final String STRIMZI_LOG_LEVEL_ENV = "STRIMZI_DEFAULT_LOG_LEVEL";
    public static final String KUBERNETES_DOMAIN_ENV = "KUBERNETES_DOMAIN";
    public static final String KUBERNETES_API_URL_ENV = "KUBERNETES_API_URL";

    public static final String STRIMZI_ORG_DEFAULT = "strimzi";
    public static final String STRIMZI_TAG_DEFAULT = "latest";
    public static final String TEST_LOG_DIR_DEFAULT = "../systemtest/target/logs/";
    public static final String ST_KAFKA_VERSION_DEFAULT = "2.1.0";
    public static final String STRIMZI_LOG_LEVEL_DEFAULT = "DEBUG";
    public static final String KUBERNETES_DOMAIN_DEFAULT = ".nip.io";
    public static final String KUBERNETES_API_URL_DEFAULT = "https://127.0.0.1:8443";
    public static final int INGRESS_DEFAULT_PORT = 4242;

    private final String strimziOrg = System.getenv().getOrDefault(STRIMZI_ORG_ENV, STRIMZI_ORG_DEFAULT);
    private final String strimziTag = System.getenv().getOrDefault(STRIMZI_TAG_ENV, STRIMZI_TAG_DEFAULT);
    private final String testLogDir = System.getenv().getOrDefault(TEST_LOG_DIR_ENV, TEST_LOG_DIR_DEFAULT);
    private final String stKafkVersion = System.getenv().getOrDefault(ST_KAFKA_VERSION_ENV, ST_KAFKA_VERSION_DEFAULT);
    private final String strimziLogLevel = System.getenv().getOrDefault(STRIMZI_LOG_LEVEL_ENV, STRIMZI_LOG_LEVEL_DEFAULT);
    private final String kubernetesDomain = System.getenv().getOrDefault(KUBERNETES_DOMAIN_ENV, KUBERNETES_DOMAIN_DEFAULT);
    private final String kubernetesApiUrl = System.getenv().getOrDefault(KUBERNETES_API_URL_ENV, KUBERNETES_API_URL_DEFAULT);

    private Environment() {
        String debugFormat = "{}:{}";
        LOGGER.info("Used environment variables:");
        LOGGER.info(debugFormat, STRIMZI_ORG_ENV, strimziOrg);
        LOGGER.info(debugFormat, STRIMZI_TAG_ENV, strimziTag);
        LOGGER.info(debugFormat, TEST_LOG_DIR_ENV, testLogDir);
        LOGGER.info(debugFormat, ST_KAFKA_VERSION_ENV, stKafkVersion);
        LOGGER.info(debugFormat, STRIMZI_LOG_LEVEL_ENV, strimziLogLevel);
        LOGGER.info(debugFormat, KUBERNETES_DOMAIN_ENV, kubernetesDomain);
        LOGGER.info(debugFormat, KUBERNETES_API_URL_ENV, kubernetesApiUrl);
    }

    public static synchronized Environment getInstance() {
        if (instance == null) {
            instance = new Environment();
        }
        return instance;
    }

    public String getStrimziOrg() {
        return strimziOrg;
    }

    public String getStrimziTag() {
        return strimziTag;
    }

    public String getTestLogDir() {
        return testLogDir;
    }

    public String getStKafkaVersionEnv() {
        return stKafkVersion;
    }

    public String getStrimziLogLevel() {
        return strimziLogLevel;
    }

    public String getKubernetesDomain() {
        return kubernetesDomain;
    }

    public String getKubernetesApiUrl() {
        return kubernetesApiUrl;
    }
}
