/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Class which holds environment variables for system tests.
 */
public class Environment {

    private static final Logger LOGGER = LogManager.getLogger(Environment.class);
    private static final Map<String, String> VALUES = new HashMap<>();
    private static final JsonNode JSON_DATA = loadConfigurationFile();

    /**
     * Specify the system test configuration file path from an environmental variable
     */
    private static final String CONFIG_FILE_PATH_ENVAR = "ST_CONFIG_PATH";
    /**
     * Specify secret name of private registries, with the container registry credentials to be able pull images.
     */
    private static final String STRIMZI_IMAGE_PULL_SECRET_ENV = "SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET";
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

    private static final String ST_KAFKA_VERSION_DEFAULT = "2.6.0";
    public static final String STRIMZI_ORG_DEFAULT = "strimzi";
    public static final String STRIMZI_TAG_DEFAULT = "latest";
    public static final String STRIMZI_REGISTRY_DEFAULT = "docker.io";
    private static final String TEST_LOG_DIR_DEFAULT = TestUtils.USER_PATH + "/../systemtest/target/logs/";
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

    private static String config;
    public static final String SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET = getOrDefault(STRIMZI_IMAGE_PULL_SECRET_ENV, "");
    public static final String STRIMZI_ORG = getOrDefault(STRIMZI_ORG_ENV, STRIMZI_ORG_DEFAULT);
    public static final String STRIMZI_TAG = getOrDefault(STRIMZI_TAG_ENV, STRIMZI_TAG_DEFAULT);
    public static final String STRIMZI_REGISTRY = getOrDefault(STRIMZI_REGISTRY_ENV, STRIMZI_REGISTRY_DEFAULT);
    public static final String TEST_LOG_DIR = getOrDefault(TEST_LOG_DIR_ENV, TEST_LOG_DIR_DEFAULT);
    public static final String ST_KAFKA_VERSION = getOrDefault(ST_KAFKA_VERSION_ENV, ST_KAFKA_VERSION_DEFAULT);
    public static final String STRIMZI_LOG_LEVEL = getOrDefault(STRIMZI_LOG_LEVEL_ENV, STRIMZI_LOG_LEVEL_DEFAULT);
    public static final String KUBERNETES_DOMAIN = getOrDefault(KUBERNETES_DOMAIN_ENV, KUBERNETES_DOMAIN_DEFAULT);
    public static final boolean SKIP_TEARDOWN = getOrDefault(SKIP_TEARDOWN_ENV, Boolean::parseBoolean, false);
    // variables for test-client image
    private static final String TEST_CLIENT_IMAGE_DEFAULT = STRIMZI_REGISTRY + "/" + STRIMZI_ORG + "/test-client:" + STRIMZI_TAG + "-kafka-" + ST_KAFKA_VERSION;
    public static final String TEST_CLIENT_IMAGE = getOrDefault(TEST_CLIENT_IMAGE_ENV, TEST_CLIENT_IMAGE_DEFAULT);
    // variables for kafka bridge image
    private static final String BRIDGE_IMAGE_DEFAULT = "latest-released";
    public static final String BRIDGE_IMAGE = getOrDefault(BRIDGE_IMAGE_ENV, BRIDGE_IMAGE_DEFAULT);
    // Image pull policy variables
    public static final String COMPONENTS_IMAGE_PULL_POLICY = getOrDefault(COMPONENTS_IMAGE_PULL_POLICY_ENV, COMPONENTS_IMAGE_PULL_POLICY_ENV_DEFAULT);
    public static final String OPERATOR_IMAGE_PULL_POLICY = getOrDefault(OPERATOR_IMAGE_PULL_POLICY_ENV, OPERATOR_IMAGE_PULL_POLICY_ENV_DEFAULT);
    // OLM env variables
    public static final String OLM_OPERATOR_NAME = getOrDefault(OLM_OPERATOR_NAME_ENV, OLM_OPERATOR_NAME_DEFAULT);
    public static final String OLM_SOURCE_NAME = getOrDefault(OLM_SOURCE_NAME_ENV, OLM_SOURCE_NAME_DEFAULT);
    public static final String OLM_APP_BUNDLE_PREFIX = getOrDefault(OLM_APP_BUNDLE_PREFIX_ENV, OLM_APP_BUNDLE_PREFIX_DEFAULT);
    public static final String OLM_OPERATOR_VERSION = getOrDefault(OLM_OPERATOR_VERSION_ENV, OLM_OPERATOR_VERSION_DEFAULT);
    // NetworkPolicy variable
    public static final String DEFAULT_TO_DENY_NETWORK_POLICIES = getOrDefault(DEFAULT_TO_DENY_NETWORK_POLICIES_ENV, DEFAULT_TO_DENY_NETWORK_POLICIES_DEFAULT);
    // ClusterOperator installation type variable
    public static final String CLUSTER_OPERATOR_INSTALL_TYPE = getOrDefault(CLUSTER_OPERATOR_INSTALL_TYPE_ENV, CLUSTER_OPERATOR_INSTALL_TYPE_DEFAULT);

    private Environment() { }

    static {
        String debugFormat = "{}: {}";
        LOGGER.info("Used environment variables:");
        LOGGER.info(debugFormat, "CONFIG", config);
        VALUES.forEach((key, value) -> LOGGER.info(debugFormat, key, value));
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

    private static String getOrDefault(String varName, String defaultValue) {
        return getOrDefault(varName, String::toString, defaultValue);
    }

    private static <T> T getOrDefault(String var, Function<String, T> converter, T defaultValue) {
        String value = System.getenv(var) != null ?
                System.getenv(var) :
                (Objects.requireNonNull(JSON_DATA).get(var) != null ?
                        JSON_DATA.get(var).asText() :
                        null);
        T returnValue = defaultValue;
        if (value != null) {
            returnValue = converter.apply(value);
        }
        VALUES.put(var, String.valueOf(returnValue));
        return returnValue;
    }

    private static JsonNode loadConfigurationFile() {
        config = System.getenv().getOrDefault(CONFIG_FILE_PATH_ENVAR,
                Paths.get(System.getProperty("user.dir"), "config.json").toAbsolutePath().toString());
        ObjectMapper mapper = new ObjectMapper();
        try {
            File jsonFile = new File(config).getAbsoluteFile();
            return mapper.readTree(jsonFile);
        } catch (IOException ex) {
            LOGGER.info("Json configuration not provider or not exists");
            return mapper.createObjectNode();
        }
    }
}
