/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Service;
import io.skodjob.kubetest4j.enums.InstallType;
import io.skodjob.kubetest4j.environment.TestEnvironmentVariables;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.systemtest.enums.ClusterOperatorRBACType;
import io.strimzi.systemtest.utils.TestKafkaVersion;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.OpenShift;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Locale;

/**
 * Class which holds environment variables for system tests.
 */
public class Environment {

    private static final Logger LOGGER = LogManager.getLogger(Environment.class);
    private static final TestEnvironmentVariables ENVIRONMENT_VARIABLES = new TestEnvironmentVariables();

    /**
     * Specify Secret name of private registries, with the container registry credentials to be able to pull images.
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

    private static final String SCRAPER_IMAGE_ENV = "SCRAPER_IMAGE";

    /**
     * Specify kafka bridge image used in system tests.
     */
    private static final String BRIDGE_IMAGE_ENV = "BRIDGE_IMAGE";
    /**
     * Directory for store logs collected during the tests.
     */
    private static final String TEST_LOG_DIR_ENV = "TEST_LOG_DIR";
    private static final String PERFORMANCE_DIR_ENV = "PERFORMANCE_DIR";
    /**
     * Kafka version used in images during the system tests.
     */
    private static final String ST_KAFKA_VERSION_ENV = "ST_KAFKA_VERSION";
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

    /**
     * OLM env variables
     */
    private static final String OLM_OPERATOR_NAME_ENV = "OLM_OPERATOR_NAME";
    private static final String OLM_OPERATOR_DEPLOYMENT_NAME_ENV = "OLM_OPERATOR_DEPLOYMENT_NAME";
    private static final String OLM_SOURCE_NAME_ENV = "OLM_SOURCE_NAME";
    private static final String OLM_SOURCE_NAMESPACE_ENV = "OLM_SOURCE_NAMESPACE";
    private static final String OLM_APP_BUNDLE_PREFIX_ENV = "OLM_APP_BUNDLE_PREFIX";
    private static final String OLM_OPERATOR_CHANNEL_ENV = "OLM_OPERATOR_CHANNEL";
    /**
     * Allows network policies
     */
    private static final String DEFAULT_TO_DENY_NETWORK_POLICIES_ENV = "DEFAULT_TO_DENY_NETWORK_POLICIES";
    /**
     * Cluster Operator installation type
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
     * Resource allocation strategy
     */
    public static final String RESOURCE_ALLOCATION_STRATEGY_ENV = "RESOURCE_ALLOCATION_STRATEGY";

    /**
     * User specific registry for Connect build
     */
    public static final String CONNECT_BUILD_IMAGE_PATH_ENV = "CONNECT_BUILD_IMAGE_PATH";
    public static final String CONNECT_BUILD_REGISTRY_SECRET_ENV = "CONNECT_BUILD_REGISTRY_SECRET";
    public static final String IP_FAMILY_ENV = "IP_FAMILY";

    /**
     * Connect image with file sink plugin
     */
    public static final String CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN_ENV = "CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN";

    /**
     * Env var for specify base image for building Kafka with tiered storage in system tests
     */
    public static final String KAFKA_TIERED_STORAGE_BASE_IMAGE_ENV = "KAFKA_TIERED_STORAGE_BASE_IMAGE";
    public static final String KAFKA_TIERED_STORAGE_IMAGE_ENV = "KAFKA_TIERED_STORAGE_IMAGE";
    public static final String KAFKA_TIERED_STORAGE_CLASSPATH_ENV = "KAFKA_TIERED_STORAGE_CLASSPATH";
    public static final String BUILDAH_IMAGE_ENV = "BUILDAH_IMAGE";

    public static final String POSTGRES_IMAGE_ENV = "POSTGRES_IMAGE";

    /**
     * Defaults
     */
    public static final String STRIMZI_TAG_DEFAULT = "latest";
    public static final String STRIMZI_ORG_DEFAULT = "strimzi";
    public static final String STRIMZI_REGISTRY_DEFAULT = "quay.io";
    private static final String TEST_LOG_DIR_DEFAULT = TestUtils.USER_PATH + "/../systemtest/target/logs/";
    private static final String PERFORMANCE_DIR_DEFAULT = TestUtils.USER_PATH + "/../systemtest/target/performance/";
    private static final String STRIMZI_LOG_LEVEL_DEFAULT = "DEBUG";
    public static final String COMPONENTS_IMAGE_PULL_POLICY_ENV_DEFAULT = TestConstants.IF_NOT_PRESENT_IMAGE_PULL_POLICY;
    public static final String OPERATOR_IMAGE_PULL_POLICY_ENV_DEFAULT = TestConstants.ALWAYS_IMAGE_PULL_POLICY;
    public static final String OLM_OPERATOR_NAME_DEFAULT = "strimzi-kafka-operator";
    public static final String OLM_OPERATOR_DEPLOYMENT_NAME_DEFAULT = TestConstants.STRIMZI_DEPLOYMENT_NAME;
    public static final String OLM_SOURCE_NAME_DEFAULT = "community-operators";
    public static final String OLM_APP_BUNDLE_PREFIX_DEFAULT = "strimzi-cluster-operator";
    public static final String OLM_OPERATOR_CHANNEL_DEFAULT = "stable";
    private static final boolean DEFAULT_TO_DENY_NETWORK_POLICIES_DEFAULT = true;
    private static final boolean LB_FINALIZERS_DEFAULT = false;
    private static final String STRIMZI_FEATURE_GATES_DEFAULT = "";
    private static final String RESOURCE_ALLOCATION_STRATEGY_DEFAULT = "SHARE_MEMORY_FOR_ALL_COMPONENTS";

    private static final String ST_KAFKA_VERSION_DEFAULT = TestKafkaVersion.getDefaultSupportedKafkaVersion();
    public static final String ST_FILE_PLUGIN_URL_DEFAULT = "https://repo1.maven.org/maven2/org/apache/kafka/connect-file/" + ST_KAFKA_VERSION_DEFAULT + "/connect-file-" + ST_KAFKA_VERSION_DEFAULT + ".jar";

    public static final String IP_FAMILY_DEFAULT = "ipv4";
    public static final String IP_FAMILY_VERSION_6 = "ipv6";
    public static final String IP_FAMILY_DUAL_STACK = "dual";

    public static final String KAFKA_TIERED_STORAGE_BASE_IMAGE_DEFAULT = STRIMZI_REGISTRY_DEFAULT + "/" + STRIMZI_ORG_DEFAULT + "/kafka:latest-kafka-" + ST_KAFKA_VERSION_DEFAULT;
    public static final String KAFKA_TIERED_STORAGE_CLASSPATH_DEFAULT = "/opt/kafka/plugins/tiered-storage/*";
    public static final String BUILDAH_IMAGE_DEFAULT = "quay.io/containers/buildah:v1.41.4";

    public static final String POSTGRES_IMAGE_DEFAULT = "docker.io/library/postgres:18.0";

    /**
     * Set values
     */
    public static final String CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN = ENVIRONMENT_VARIABLES.getOrDefault(CONNECT_IMAGE_WITH_FILE_SINK_PLUGIN_ENV, "");
    public static final String SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET = ENVIRONMENT_VARIABLES.getOrDefault(STRIMZI_IMAGE_PULL_SECRET_ENV, "");
    public static final String STRIMZI_ORG = ENVIRONMENT_VARIABLES.getOrDefault(STRIMZI_ORG_ENV, "");
    public static final String STRIMZI_TAG = ENVIRONMENT_VARIABLES.getOrDefault(STRIMZI_TAG_ENV, "");
    public static final String STRIMZI_REGISTRY = ENVIRONMENT_VARIABLES.getOrDefault(STRIMZI_REGISTRY_ENV, "");
    public static final String TEST_LOG_DIR = ENVIRONMENT_VARIABLES.getOrDefault(TEST_LOG_DIR_ENV, TEST_LOG_DIR_DEFAULT);
    public static final String PERFORMANCE_DIR = ENVIRONMENT_VARIABLES.getOrDefault(PERFORMANCE_DIR_ENV, PERFORMANCE_DIR_DEFAULT);
    public static final String ST_KAFKA_VERSION = ENVIRONMENT_VARIABLES.getOrDefault(ST_KAFKA_VERSION_ENV, ST_KAFKA_VERSION_DEFAULT);
    public static final String STRIMZI_LOG_LEVEL = ENVIRONMENT_VARIABLES.getOrDefault(STRIMZI_LOG_LEVEL_ENV, STRIMZI_LOG_LEVEL_DEFAULT);
    public static final boolean SKIP_TEARDOWN = ENVIRONMENT_VARIABLES.getOrDefault(SKIP_TEARDOWN_ENV, Boolean::parseBoolean, false);
    public static final ClusterOperatorRBACType STRIMZI_RBAC_SCOPE =  ENVIRONMENT_VARIABLES.getOrDefault(STRIMZI_RBAC_SCOPE_ENV, value -> ClusterOperatorRBACType.valueOf(value.toUpperCase(Locale.ENGLISH)), ClusterOperatorRBACType.CLUSTER);

    public static final String STRIMZI_FEATURE_GATES = ENVIRONMENT_VARIABLES.getOrDefault(STRIMZI_FEATURE_GATES_ENV, STRIMZI_FEATURE_GATES_DEFAULT);

    private static final String SCRAPER_IMAGE_DEFAULT = getIfNotEmptyOrDefault(STRIMZI_REGISTRY, STRIMZI_REGISTRY_DEFAULT) + "/" +
        getIfNotEmptyOrDefault(STRIMZI_ORG, STRIMZI_ORG_DEFAULT) + "/kafka:" +
        getIfNotEmptyOrDefault(STRIMZI_TAG, STRIMZI_TAG_DEFAULT) + "-kafka-" + ST_KAFKA_VERSION;
    public static final String SCRAPER_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(SCRAPER_IMAGE_ENV, SCRAPER_IMAGE_DEFAULT);

    // variables for kafka bridge image
    private static final String BRIDGE_IMAGE_DEFAULT = "latest-released";
    public static final String BRIDGE_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(BRIDGE_IMAGE_ENV, BRIDGE_IMAGE_DEFAULT);
    // Image pull policy variables
    public static final String COMPONENTS_IMAGE_PULL_POLICY = ENVIRONMENT_VARIABLES.getOrDefault(COMPONENTS_IMAGE_PULL_POLICY_ENV, COMPONENTS_IMAGE_PULL_POLICY_ENV_DEFAULT);
    public static final String OPERATOR_IMAGE_PULL_POLICY = ENVIRONMENT_VARIABLES.getOrDefault(OPERATOR_IMAGE_PULL_POLICY_ENV, OPERATOR_IMAGE_PULL_POLICY_ENV_DEFAULT);
    // OLM env variables
    public static final String OLM_OPERATOR_NAME = ENVIRONMENT_VARIABLES.getOrDefault(OLM_OPERATOR_NAME_ENV, OLM_OPERATOR_NAME_DEFAULT);
    public static final String OLM_OPERATOR_DEPLOYMENT_NAME = ENVIRONMENT_VARIABLES.getOrDefault(OLM_OPERATOR_DEPLOYMENT_NAME_ENV, OLM_OPERATOR_DEPLOYMENT_NAME_DEFAULT);
    public static final String OLM_SOURCE_NAME = ENVIRONMENT_VARIABLES.getOrDefault(OLM_SOURCE_NAME_ENV, OLM_SOURCE_NAME_DEFAULT);
    public static final String OLM_SOURCE_NAMESPACE = ENVIRONMENT_VARIABLES.getOrDefault(OLM_SOURCE_NAMESPACE_ENV, OpenShift.OLM_SOURCE_NAMESPACE);
    public static final String OLM_APP_BUNDLE_PREFIX = ENVIRONMENT_VARIABLES.getOrDefault(OLM_APP_BUNDLE_PREFIX_ENV, OLM_APP_BUNDLE_PREFIX_DEFAULT);
    public static final String OLM_OPERATOR_CHANNEL = ENVIRONMENT_VARIABLES.getOrDefault(OLM_OPERATOR_CHANNEL_ENV, OLM_OPERATOR_CHANNEL_DEFAULT);
    // NetworkPolicy variable
    public static final boolean DEFAULT_TO_DENY_NETWORK_POLICIES = ENVIRONMENT_VARIABLES.getOrDefault(DEFAULT_TO_DENY_NETWORK_POLICIES_ENV, Boolean::parseBoolean, DEFAULT_TO_DENY_NETWORK_POLICIES_DEFAULT);
    public static final InstallType CLUSTER_OPERATOR_INSTALL_TYPE = ENVIRONMENT_VARIABLES.getOrDefault(CLUSTER_OPERATOR_INSTALL_TYPE_ENV, InstallType::fromString, InstallType.Yaml);

    public static final boolean LB_FINALIZERS = ENVIRONMENT_VARIABLES.getOrDefault(LB_FINALIZERS_ENV, Boolean::parseBoolean, LB_FINALIZERS_DEFAULT);
    public static final String RESOURCE_ALLOCATION_STRATEGY = ENVIRONMENT_VARIABLES.getOrDefault(RESOURCE_ALLOCATION_STRATEGY_ENV, RESOURCE_ALLOCATION_STRATEGY_DEFAULT);

    // Connect build related variables
    public static final String ST_FILE_PLUGIN_URL = ENVIRONMENT_VARIABLES.getOrDefault(ST_FILE_PLUGIN_URL_ENV, ST_FILE_PLUGIN_URL_DEFAULT);

    public static final String CONNECT_BUILD_IMAGE_PATH = ENVIRONMENT_VARIABLES.getOrDefault(CONNECT_BUILD_IMAGE_PATH_ENV, "");
    public static final String CONNECT_BUILD_REGISTRY_SECRET = ENVIRONMENT_VARIABLES.getOrDefault(CONNECT_BUILD_REGISTRY_SECRET_ENV, "");
    public static final String TEST_SUITE_NAMESPACE = Environment.isNamespaceRbacScope() ? TestConstants.CO_NAMESPACE : "test-suite-namespace";

    public static final String IP_FAMILY = ENVIRONMENT_VARIABLES.getOrDefault(IP_FAMILY_ENV, IP_FAMILY_DEFAULT);

    public static final String KAFKA_TIERED_STORAGE_BASE_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(KAFKA_TIERED_STORAGE_BASE_IMAGE_ENV, KAFKA_TIERED_STORAGE_BASE_IMAGE_DEFAULT);
    public static final String BUILDAH_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(BUILDAH_IMAGE_ENV, BUILDAH_IMAGE_DEFAULT);
    public static final String KAFKA_TIERED_STORAGE_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(KAFKA_TIERED_STORAGE_IMAGE_ENV, "");
    public static final String KAFKA_TIERED_STORAGE_CLASSPATH = ENVIRONMENT_VARIABLES.getOrDefault(KAFKA_TIERED_STORAGE_CLASSPATH_ENV, KAFKA_TIERED_STORAGE_CLASSPATH_DEFAULT);

    public static final String POSTGRES_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(POSTGRES_IMAGE_ENV, POSTGRES_IMAGE_DEFAULT);

    private Environment() { }

    static {
        ENVIRONMENT_VARIABLES.logEnvironmentVariables();
        try {
            ENVIRONMENT_VARIABLES.saveConfigurationFile(TEST_LOG_DIR);
        } catch (IOException e) {
            LOGGER.error("Failed to write configuration to the {} due to: {}", TEST_LOG_DIR, e);
        }
    }

    public static boolean isOlmInstall() {
        return CLUSTER_OPERATOR_INSTALL_TYPE.equals(InstallType.Olm);
    }

    public static boolean isHelmInstall() {
        return CLUSTER_OPERATOR_INSTALL_TYPE.equals(InstallType.Helm);
    }

    public static boolean isNamespaceRbacScope() {
        return ClusterOperatorRBACType.NAMESPACE.equals(STRIMZI_RBAC_SCOPE);
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

    public static boolean isIpv4Family() {
        return IP_FAMILY.contains(IP_FAMILY_DEFAULT);
    }

    public static boolean isIpv6Family() {
        return IP_FAMILY.contains(IP_FAMILY_VERSION_6);
    }

    public static boolean isDualStackIpFamily() {
        return IP_FAMILY.contains(IP_FAMILY_DUAL_STACK);
    }

    public static boolean isConnectBuildWithBuildahEnabled() {
        return !STRIMZI_FEATURE_GATES.contains("-UseConnectBuildWithBuildah");
    }

    public static String getImageOutputRegistry() {
        if (KubeClusterResource.getInstance().isOpenShift()) {
            return "image-registry.openshift-image-registry.svc:5000";
        } else if (KubeClusterResource.getInstance().isKind()) {
            // we will need a hostname of machine
            String hostname = getHostname();
            LOGGER.info("Using container registry '{}'", hostname);
            return hostname;
        } else {
            LOGGER.warn("For running these tests on K8s you have to have internal registry deployed using `minikube start --insecure-registry '10.0.0.0/24'` and `minikube addons enable registry`");
            Service service = KubeResourceManager.get().kubeClient().getClient().services().inNamespace("kube-system").withName("registry").get();

            if (service == null)    {
                throw new RuntimeException("Internal registry Service for pushing newly build images not found.");
            } else {
                return service.getSpec().getClusterIP() + ":" + service.getSpec().getPorts().stream().filter(servicePort -> servicePort.getName().equals("http")).findFirst().orElseThrow().getPort();
            }
        }
    }

    private static String getHostname() {
        String hostname = "";
        try {
            if (Environment.isIpv4Family() || Environment.isDualStackIpFamily()) {
                hostname = InetAddress.getLocalHost().getHostAddress() + ":5001";
            } else if (Environment.isIpv6Family()) {
                hostname = "myregistry.local:5001";
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        return hostname;
    }

    public static String getImageOutputRegistry(String namespaceName, String imageName, String tag) {
        if (!Environment.CONNECT_BUILD_IMAGE_PATH.isEmpty()) {
            return Environment.CONNECT_BUILD_IMAGE_PATH + ":" + tag;
        } else {
            return getImageOutputRegistry() + "/" + namespaceName + "/" + imageName + ":" + tag;
        }
    }

    public static String getIfNotEmptyOrDefault(String envVar, String defaultValue) {
        return envVar.isEmpty() ? defaultValue : envVar;
    }
}