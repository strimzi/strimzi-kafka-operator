/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.systemtest.enums.ClusterOperatorInstallType;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.OpenShift;
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

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

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
    private static final String CONFIG_FILE_PATH_ENV = "ST_CONFIG_PATH";
    /**
     * Specify secret name of private registries, with the container registry credentials to be able to pull images.
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
     * Specify Kafka client app images used in system tests.
     */
    private static final String TEST_PRODUCER_IMAGE_ENV = "TEST_PRODUCER_IMAGE";
    private static final String TEST_CONSUMER_IMAGE_ENV = "TEST_CONSUMER_IMAGE";
    private static final String TEST_STREAMS_IMAGE_ENV = "TEST_STREAMS_IMAGE";
    private static final String TEST_ADMIN_IMAGE_ENV = "TEST_ADMIN_IMAGE";
    private static final String TEST_HTTP_PRODUCER_IMAGE_ENV = "TEST_HTTP_PRODUCER_IMAGE";
    private static final String TEST_HTTP_CONSUMER_IMAGE_ENV = "TEST_HTTP_CONSUMER_IMAGE";
    private static final String TEST_CLIENTS_VERSION_ENV = "TEST_CLIENTS_VERSION";

    private static final String SCRAPER_IMAGE_ENV = "SCRAPER_IMAGE";

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
     * Kafka version used in test-clients during the system tests.
     */
    private static final String CLIENTS_KAFKA_VERSION_ENV = "CLIENTS_KAFKA_VERSION";
    /**
     * Log level for cluster operator.
     */
    private static final String STRIMZI_LOG_LEVEL_ENV = "STRIMZI_LOG_LEVEL";
    /**
     * Image pull policy env var for Components images (Kafka, Bridge, ...)
     */
    private static final String COMPONENTS_IMAGE_PULL_POLICY_ENV = "COMPONENTS_IMAGE_PULL_POLICY";
    /**
     * Image pull policy env var for Operator images
     */
    private static final String OPERATOR_IMAGE_PULL_POLICY_ENV = "OPERATOR_IMAGE_PULL_POLICY";
    /**
     * CO Roles only mode.
     */
    public static final String STRIMZI_RBAC_SCOPE_ENV = "STRIMZI_RBAC_SCOPE";
    public static final String STRIMZI_RBAC_SCOPE_CLUSTER = "CLUSTER";
    public static final String STRIMZI_RBAC_SCOPE_NAMESPACE = "NAMESPACE";
    public static final String STRIMZI_RBAC_SCOPE_DEFAULT = STRIMZI_RBAC_SCOPE_CLUSTER;

    /**
     * OLM env variables
     */
    private static final String OLM_OPERATOR_NAME_ENV = "OLM_OPERATOR_NAME";
    private static final String OLM_OPERATOR_DEPLOYMENT_NAME_ENV = "OLM_OPERATOR_DEPLOYMENT_NAME";
    private static final String OLM_SOURCE_NAME_ENV = "OLM_SOURCE_NAME";
    private static final String OLM_SOURCE_NAMESPACE_ENV = "OLM_SOURCE_NAMESPACE";
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

    /**
     * Use finalizers for loadbalancers
     */
    private static final String LB_FINALIZERS_ENV = "LB_FINALIZERS";

    /**
     * CO Features gates variable
     */
    public static final String STRIMZI_FEATURE_GATES_ENV = "STRIMZI_FEATURE_GATES";

    /**
     * CO PodSet-only reconciliation env variable <br>
     * Only SPS will be reconciled, when this env variable will be true
     */
    public static final String STRIMZI_POD_SET_RECONCILIATION_ONLY_ENV = "STRIMZI_POD_SET_RECONCILIATION_ONLY";

    public static final String ST_FILE_PLUGIN_URL_ENV = "ST_FILE_SINK_PLUGIN_URL";

    /**
     * CO Features gates variable
     */
    public static final String RESOURCE_ALLOCATION_STRATEGY_ENV = "RESOURCE_ALLOCATION_STRATEGY";

    /**
     * Defaults
     */
    public static final String STRIMZI_ORG_DEFAULT = "strimzi";
    public static final String STRIMZI_TAG_DEFAULT = "latest";
    public static final String STRIMZI_REGISTRY_DEFAULT = "quay.io";
    public static final String TEST_CLIENTS_ORG_DEFAULT = "strimzi-test-clients";
    private static final String TEST_LOG_DIR_DEFAULT = TestUtils.USER_PATH + "/../systemtest/target/logs/";
    private static final String STRIMZI_LOG_LEVEL_DEFAULT = "DEBUG";
    public static final String COMPONENTS_IMAGE_PULL_POLICY_ENV_DEFAULT = Constants.IF_NOT_PRESENT_IMAGE_PULL_POLICY;
    public static final String OPERATOR_IMAGE_PULL_POLICY_ENV_DEFAULT = Constants.ALWAYS_IMAGE_PULL_POLICY;
    public static final String OLM_OPERATOR_NAME_DEFAULT = "strimzi-kafka-operator";
    public static final String OLM_OPERATOR_DEPLOYMENT_NAME_DEFAULT = Constants.STRIMZI_DEPLOYMENT_NAME;
    public static final String OLM_SOURCE_NAME_DEFAULT = "community-operators";
    public static final String OLM_APP_BUNDLE_PREFIX_DEFAULT = "strimzi-cluster-operator";
    private static final boolean DEFAULT_TO_DENY_NETWORK_POLICIES_DEFAULT = true;
    private static final ClusterOperatorInstallType CLUSTER_OPERATOR_INSTALL_TYPE_DEFAULT = ClusterOperatorInstallType.BUNDLE;
    private static final boolean LB_FINALIZERS_DEFAULT = false;
    private static final String STRIMZI_FEATURE_GATES_DEFAULT = "";
    private static final String RESOURCE_ALLOCATION_STRATEGY_DEFAULT = "SHARE_MEMORY_FOR_ALL_COMPONENTS";

    private static final String ST_KAFKA_VERSION_DEFAULT = TestKafkaVersion.getDefaultSupportedKafkaVersion();
    private static final String ST_CLIENTS_KAFKA_VERSION_DEFAULT = "3.3.1";
    public static final String TEST_CLIENTS_VERSION_DEFAULT = "0.4.2";
    public static final String ST_FILE_PLUGIN_URL_DEFAULT = "https://repo1.maven.org/maven2/org/apache/kafka/connect-file/" + ST_KAFKA_VERSION_DEFAULT + "/connect-file-" + ST_KAFKA_VERSION_DEFAULT + ".jar";

    /**
     * Set values
     */
    private static String config;
    public static final String SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET = getOrDefault(STRIMZI_IMAGE_PULL_SECRET_ENV, "");
    public static final String STRIMZI_ORG = getOrDefault(STRIMZI_ORG_ENV, STRIMZI_ORG_DEFAULT);
    public static final String STRIMZI_TAG = getOrDefault(STRIMZI_TAG_ENV, STRIMZI_TAG_DEFAULT);
    public static final String STRIMZI_REGISTRY = getOrDefault(STRIMZI_REGISTRY_ENV, STRIMZI_REGISTRY_DEFAULT);
    public static final String TEST_LOG_DIR = getOrDefault(TEST_LOG_DIR_ENV, TEST_LOG_DIR_DEFAULT);
    public static final String ST_KAFKA_VERSION = getOrDefault(ST_KAFKA_VERSION_ENV, ST_KAFKA_VERSION_DEFAULT);
    public static final String CLIENTS_KAFKA_VERSION = getOrDefault(CLIENTS_KAFKA_VERSION_ENV, ST_CLIENTS_KAFKA_VERSION_DEFAULT);
    public static final String STRIMZI_LOG_LEVEL = getOrDefault(STRIMZI_LOG_LEVEL_ENV, STRIMZI_LOG_LEVEL_DEFAULT);
    public static final boolean SKIP_TEARDOWN = getOrDefault(SKIP_TEARDOWN_ENV, Boolean::parseBoolean, false);
    public static final String STRIMZI_RBAC_SCOPE = getOrDefault(STRIMZI_RBAC_SCOPE_ENV, STRIMZI_RBAC_SCOPE_DEFAULT);
    public static final String STRIMZI_FEATURE_GATES = getOrDefault(STRIMZI_FEATURE_GATES_ENV, STRIMZI_FEATURE_GATES_DEFAULT);

    // variables for kafka client app images
    private static final String TEST_CLIENTS_VERSION = getOrDefault(TEST_CLIENTS_VERSION_ENV, TEST_CLIENTS_VERSION_DEFAULT);
    private static final String TEST_PRODUCER_IMAGE_DEFAULT = STRIMZI_REGISTRY_DEFAULT + "/" + TEST_CLIENTS_ORG_DEFAULT + "/test-client-kafka-producer:" + TEST_CLIENTS_VERSION + "-kafka-" + CLIENTS_KAFKA_VERSION;
    private static final String TEST_CONSUMER_IMAGE_DEFAULT = STRIMZI_REGISTRY_DEFAULT + "/" + TEST_CLIENTS_ORG_DEFAULT + "/test-client-kafka-consumer:" + TEST_CLIENTS_VERSION + "-kafka-" + CLIENTS_KAFKA_VERSION;
    private static final String TEST_STREAMS_IMAGE_DEFAULT = STRIMZI_REGISTRY_DEFAULT + "/" + TEST_CLIENTS_ORG_DEFAULT + "/test-client-kafka-streams:" + TEST_CLIENTS_VERSION + "-kafka-" + CLIENTS_KAFKA_VERSION;
    private static final String TEST_ADMIN_IMAGE_DEFAULT = STRIMZI_REGISTRY_DEFAULT + "/" + TEST_CLIENTS_ORG_DEFAULT + "/test-client-kafka-admin:" + TEST_CLIENTS_VERSION + "-kafka-" + CLIENTS_KAFKA_VERSION;
    private static final String TEST_HTTP_PRODUCER_IMAGE_DEFAULT = STRIMZI_REGISTRY_DEFAULT + "/" + TEST_CLIENTS_ORG_DEFAULT + "/test-client-http-producer:" + TEST_CLIENTS_VERSION;
    private static final String TEST_HTTP_CONSUMER_IMAGE_DEFAULT = STRIMZI_REGISTRY_DEFAULT + "/" + TEST_CLIENTS_ORG_DEFAULT + "/test-client-http-consumer:" + TEST_CLIENTS_VERSION;

    public static final String TEST_PRODUCER_IMAGE = getOrDefault(TEST_PRODUCER_IMAGE_ENV, TEST_PRODUCER_IMAGE_DEFAULT);
    public static final String TEST_CONSUMER_IMAGE = getOrDefault(TEST_CONSUMER_IMAGE_ENV, TEST_CONSUMER_IMAGE_DEFAULT);
    public static final String TEST_STREAMS_IMAGE = getOrDefault(TEST_STREAMS_IMAGE_ENV, TEST_STREAMS_IMAGE_DEFAULT);
    public static final String TEST_ADMIN_IMAGE = getOrDefault(TEST_ADMIN_IMAGE_ENV, TEST_ADMIN_IMAGE_DEFAULT);
    public static final String TEST_HTTP_PRODUCER_IMAGE = getOrDefault(TEST_HTTP_PRODUCER_IMAGE_ENV, TEST_HTTP_PRODUCER_IMAGE_DEFAULT);
    public static final String TEST_HTTP_CONSUMER_IMAGE = getOrDefault(TEST_HTTP_CONSUMER_IMAGE_ENV, TEST_HTTP_CONSUMER_IMAGE_DEFAULT);

    private static final String SCRAPER_IMAGE_DEFAULT = STRIMZI_REGISTRY + "/" + STRIMZI_ORG + "/kafka:" + STRIMZI_TAG + "-kafka-" + ST_KAFKA_VERSION;
    public static final String SCRAPER_IMAGE = getOrDefault(SCRAPER_IMAGE_ENV, SCRAPER_IMAGE_DEFAULT);

    // variables for kafka bridge image
    private static final String BRIDGE_IMAGE_DEFAULT = "latest-released";
    public static final String BRIDGE_IMAGE = getOrDefault(BRIDGE_IMAGE_ENV, BRIDGE_IMAGE_DEFAULT);
    // Image pull policy variables
    public static final String COMPONENTS_IMAGE_PULL_POLICY = getOrDefault(COMPONENTS_IMAGE_PULL_POLICY_ENV, COMPONENTS_IMAGE_PULL_POLICY_ENV_DEFAULT);
    public static final String OPERATOR_IMAGE_PULL_POLICY = getOrDefault(OPERATOR_IMAGE_PULL_POLICY_ENV, OPERATOR_IMAGE_PULL_POLICY_ENV_DEFAULT);
    // OLM env variables
    public static final String OLM_OPERATOR_NAME = getOrDefault(OLM_OPERATOR_NAME_ENV, OLM_OPERATOR_NAME_DEFAULT);
    public static final String OLM_OPERATOR_DEPLOYMENT_NAME = getOrDefault(OLM_OPERATOR_DEPLOYMENT_NAME_ENV, OLM_OPERATOR_DEPLOYMENT_NAME_DEFAULT);
    public static final String OLM_SOURCE_NAME = getOrDefault(OLM_SOURCE_NAME_ENV, OLM_SOURCE_NAME_DEFAULT);
    public static final String OLM_SOURCE_NAMESPACE = getOrDefault(OLM_SOURCE_NAMESPACE_ENV, OpenShift.OLM_SOURCE_NAMESPACE);
    public static final String OLM_APP_BUNDLE_PREFIX = getOrDefault(OLM_APP_BUNDLE_PREFIX_ENV, OLM_APP_BUNDLE_PREFIX_DEFAULT);
    public static final String OLM_OPERATOR_LATEST_RELEASE_VERSION = getOrDefault(OLM_OPERATOR_VERSION_ENV, "0.33.1");
    // NetworkPolicy variable
    public static final boolean DEFAULT_TO_DENY_NETWORK_POLICIES = getOrDefault(DEFAULT_TO_DENY_NETWORK_POLICIES_ENV, Boolean::parseBoolean, DEFAULT_TO_DENY_NETWORK_POLICIES_DEFAULT);
    // ClusterOperator installation type variable
    public static final ClusterOperatorInstallType CLUSTER_OPERATOR_INSTALL_TYPE = getOrDefault(CLUSTER_OPERATOR_INSTALL_TYPE_ENV, value -> ClusterOperatorInstallType.valueOf(value.toUpperCase(Locale.ENGLISH)), CLUSTER_OPERATOR_INSTALL_TYPE_DEFAULT);
    public static final boolean LB_FINALIZERS = getOrDefault(LB_FINALIZERS_ENV, Boolean::parseBoolean, LB_FINALIZERS_DEFAULT);
    public static final String RESOURCE_ALLOCATION_STRATEGY = getOrDefault(RESOURCE_ALLOCATION_STRATEGY_ENV, RESOURCE_ALLOCATION_STRATEGY_DEFAULT);

    // Connect build related variables
    public static final String ST_FILE_PLUGIN_URL = getOrDefault(ST_FILE_PLUGIN_URL_ENV, ST_FILE_PLUGIN_URL_DEFAULT);

    private Environment() { }

    static {
        String debugFormat = "{}: {}";
        LOGGER.info("Used environment variables:");
        LOGGER.info(debugFormat, "CONFIG", config);
        VALUES.forEach((key, value) -> LOGGER.info(debugFormat, key, value));
    }

    public static boolean isOlmInstall() {
        return CLUSTER_OPERATOR_INSTALL_TYPE.equals(ClusterOperatorInstallType.OLM);
    }

    public static boolean isHelmInstall() {
        return CLUSTER_OPERATOR_INSTALL_TYPE.equals(ClusterOperatorInstallType.HELM);
    }

    public static boolean isNamespaceRbacScope() {
        return STRIMZI_RBAC_SCOPE_NAMESPACE.equals(STRIMZI_RBAC_SCOPE);
    }

    public static boolean isStrimziPodSetEnabled() {
        return !STRIMZI_FEATURE_GATES.contains(Constants.USE_STRIMZI_STATEFULSETS);
    }

    /**
     * Determine wheter KRaft mode of Kafka cluster is enabled in ClusterOperator or not.
     * @return true if KRaft mode is enabled, otherwise false
     */
    public static boolean isKRaftModeEnabled() {
        return !STRIMZI_FEATURE_GATES.contains(Constants.USE_STRIMZI_STATEFULSETS) && STRIMZI_FEATURE_GATES.contains(Constants.USE_KRAFT_MODE);
    }

    /**
     * Provides boolean information, if testing environment support shared memory (i.e., environment, where all
     * components share memory). In general, we use {@link Environment#RESOURCE_ALLOCATION_STRATEGY_DEFAULT} if env {@link Environment#RESOURCE_ALLOCATION_STRATEGY_ENV}
     * is not specified.
     *
     * @return true if env {@link Environment#RESOURCE_ALLOCATION_STRATEGY_ENV} contains "SHARE_MEMORY_FOR_ALL_COMPONENTS" value, otherwise false.
     */
    public static boolean isSharedMemory() {
        return RESOURCE_ALLOCATION_STRATEGY.contains(RESOURCE_ALLOCATION_STRATEGY_DEFAULT);
    }

    public static boolean useLatestReleasedBridge() {
        return Environment.BRIDGE_IMAGE.equals(Environment.BRIDGE_IMAGE_DEFAULT);
    }

    private static String getOrDefault(String varName, String defaultValue) {
        return getOrDefault(varName, String::toString, defaultValue);
    }

    public static String getImageOutputRegistry() {
        if (KubeClusterResource.getInstance().isOpenShift()) {
            return "image-registry.openshift-image-registry.svc:5000";
        } else {
            LOGGER.warn("For running these tests on K8s you have to have internal registry deployed using `minikube start --insecure-registry '10.0.0.0/24'` and `minikube addons enable registry`");
            Service service = kubeClient("kube-system").getService("registry");

            if (service == null)    {
                throw new RuntimeException("Internal registry service for pushing newly build images not found.");
            } else {
                return service.getSpec().getClusterIP() + ":" + service.getSpec().getPorts().stream().filter(servicePort -> servicePort.getName().equals("http")).findFirst().orElseThrow().getPort();
            }
        }
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
        config = System.getenv().getOrDefault(CONFIG_FILE_PATH_ENV,
                Paths.get(System.getProperty("user.dir"), "config.json").toAbsolutePath().toString());
        ObjectMapper mapper = new ObjectMapper();
        try {
            File jsonFile = new File(config).getAbsoluteFile();
            return mapper.readTree(jsonFile);
        } catch (IOException ex) {
            LOGGER.debug("Json configuration is not provided or cannot be processed!");
            return mapper.createObjectNode();
        }
    }
}
