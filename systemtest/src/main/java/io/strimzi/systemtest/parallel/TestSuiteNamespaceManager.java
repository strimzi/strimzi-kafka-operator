/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * This class contains static information (additional namespaces will are needed for test suite) before the execution of @BeforeAll.
 * By this we can easily prepare namespace in the AbstractST class and not in the the children.
 */
public class TestSuiteNamespaceManager {

    private static final Logger LOGGER = LogManager.getLogger(TestSuiteNamespaceManager.class);
    private static final boolean NAMESPACED_RBAC = Environment.isNamespaceRbacScope();
    private static final Function<List<String>, List<String>> SUITE_NAMESPACES = (additionalSuiteNamespaces) -> {
        if (NAMESPACED_RBAC) {
            // rbac namespace we are gonna use INFRA_NAMESPACE (everything is in sequential execution)
            return Collections.singletonList(Constants.INFRA_NAMESPACE);
        } else {
            return additionalSuiteNamespaces;
        }
    };

    private static AtomicInteger counterOfNamespaces;
    private static TestSuiteNamespaceManager instance;

    /**
     * Map @code{mapOfAdditionalNamespaces} which contains all additional namespaces for each test suite. It also
     * uses @code{SUITE_NAMESPACES} function, which returns main namespace in case we have RBAC policy set to NAMESPACE
     * instead of CLUSTER. Otherwise it returns list of additional namespaces.
     */
    private Map<String, List<String>> mapOfAdditionalNamespaces;

    public synchronized static TestSuiteNamespaceManager getInstance() {
        if (instance == null) {
            instance = new TestSuiteNamespaceManager();
            instance.constructMapOfAdditionalNamespaces();
            counterOfNamespaces = new AtomicInteger(0);
        }
        return instance;
    }

    private void constructMapOfAdditionalNamespaces() {
        this.mapOfAdditionalNamespaces = new HashMap<>();
        // bridge package (test suites, which needs auxiliary namespaces)
        this.mapOfAdditionalNamespaces.put("HttpBridgeKafkaExternalListenersST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.BRIDGE_KAFKA_EXTERNAL_LISTENER_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("HttpBridgeCorsST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.BRIDGE_KAFKA_CORS_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("HttpBridgeScramShaST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.BRIDGE_SCRAM_SHA_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("HttpBridgeTlsST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.BRIDGE_HTTP_TLS_NAMESPACE)));
        // cruise control package
        this.mapOfAdditionalNamespaces.put("CruiseControlApiST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.CRUISE_CONTROL_API_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("CruiseControlConfigurationST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.CRUISE_CONTROL_CONFIGURATION_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("CruiseControlST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.CRUISE_CONTROL_NAMESPACE)));
        // kafka package
        this.mapOfAdditionalNamespaces.put("DynamicConfigurationIsolatedST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.DYNAMIC_CONFIGURATION_ISOLATED_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("DynamicConfigurationSharedST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.DYNAMIC_CONFIGURATION_SHARED_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("MultipleListenersST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.MULTIPLE_LISTENER_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("ConfigProviderST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.CONFIG_PROVIDER_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("KafkaST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.KAFKA_NAMESPACE)));
        // log package
        this.mapOfAdditionalNamespaces.put("LoggingChangeST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.LOGGING_CHANGE_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("LogSettingST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.LOG_SETTING_NAMESPACE)));
        // operators package
        this.mapOfAdditionalNamespaces.put("ThrottlingQuotaST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.THROTTLING_QUOTA_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("TopicST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.TOPIC_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("UserST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.USER_NAMESPACE)));
        // rolling update package
        this.mapOfAdditionalNamespaces.put("AlternativeReconcileTriggersST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.ALTERNATIVE_RECONCILE_TRIGGERS_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("RollingUpdateST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.ROLLING_UPDATE_NAMESPACE)));
        // security package
        this.mapOfAdditionalNamespaces.put("CustomAuthorizerST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.CUSTOM_AUTHORIZER_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("NetworkPoliciesST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.NETWORK_POLICIES_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("OpaIntegrationST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.OPA_INTEGRATION_NAMESPACE)));
        this.mapOfAdditionalNamespaces.put("SecurityST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.SECURITY_NAMESPACE)));
        // tracing package
        this.mapOfAdditionalNamespaces.put("TracingST", SUITE_NAMESPACES.apply(Collections.singletonList(Constants.TRACING_NAMESPACE)));
    }

    public void createAdditionalNamespaces(ExtensionContext extensionContext) {
        final String requiredClassName = extensionContext.getRequiredTestClass().getSimpleName();
        final List<String> namespaces = mapOfAdditionalNamespaces.get(requiredClassName);
        final String testSuite = extensionContext.getRequiredTestClass().getName();

        if (namespaces != null) {
            LOGGER.info("Content of the test suite namespaces map:\n" + mapOfAdditionalNamespaces.toString());
            LOGGER.info("Test suite `" + requiredClassName + "` creates these additional namespaces:" + namespaces.toString());

            for (String namespaceName : namespaces) {
                if (namespaceName.equals(Constants.INFRA_NAMESPACE)) {
                    continue;
                }
                KubeClusterResource.getInstance().createNamespace(CollectorElement.createCollectorElement(testSuite), namespaceName);
                NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, Collections.singletonList(namespaceName));
            }
        }
    }

    /**
     * In test cases, where Kafka cluster is deployed we always create another namespace. Such test case is then
     * annotated as @ParallelNamespaceTest. This method creates from @code{extensionContext} this type of namespace
     * and store it to the @code{KubeClusterResource} instance, which then it will be needed in the @AfterEach phase.
     * The inverse operation to this one is implement in {@link #deleteParallelNamespace(ExtensionContext)}.
     *
     * @param extensionContext unifier (id), which distinguished all other test cases
     */
    public void createParallelNamespace(ExtensionContext extensionContext) {
        final String testCase = extensionContext.getRequiredTestMethod().getName();

        // if 'parallel namespace test' we are gonna create namespace
        if (StUtils.isParallelNamespaceTest(extensionContext)) {
            // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                final String namespaceTestCase = "namespace-" + counterOfNamespaces.getAndIncrement();

                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.NAMESPACE_KEY, namespaceTestCase);
                // create namespace by
                LOGGER.info("Creating namespace:{} for test case:{}", namespaceTestCase, testCase);

                KubeClusterResource.getInstance().createNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName(), extensionContext.getRequiredTestMethod().getName()), namespaceTestCase);
                NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, Collections.singletonList(namespaceTestCase));
                if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
                    StUtils.copyImagePullSecret(namespaceTestCase);
                }
            }
        }
    }

    public void deleteAdditionalNamespaces(ExtensionContext extensionContext) {
        CollectorElement collectorElement = CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName());
        Set<String> namespacesToDelete = KubeClusterResource.getMapWithSuiteNamespaces().get(collectorElement);

        if (namespacesToDelete != null) {
            // delete namespaces for specific test suite (we can not delete in parallel because of ConcurrentModificationException)
            namespacesToDelete.forEach(ns -> {
                if (!ns.equals(Constants.INFRA_NAMESPACE)) {
                    KubeClusterResource.getInstance().deleteNamespace(collectorElement, ns);
                }
            });
        }
    }

    /**
     * Analogically to the {@link #createParallelNamespace(ExtensionContext)}.
     *
     * @param extensionContext unifier (id), which distinguished all other test cases
     */
    public void deleteParallelNamespace(ExtensionContext extensionContext) {
        // if 'parallel namespace test' we are gonna delete namespace
        if (StUtils.isParallelNamespaceTest(extensionContext)) {
            // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                final String namespaceToDelete = extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.NAMESPACE_KEY).toString();

                LOGGER.info("Deleting namespace:{} for test case:{}", namespaceToDelete, extensionContext.getDisplayName());
                KubeClusterResource.getInstance().deleteNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName(), extensionContext.getRequiredTestMethod().getName()), namespaceToDelete);
            }
        }
    }

    public Map<String, List<String>> getMapOfAdditionalNamespaces() {
        return mapOfAdditionalNamespaces;
    }
}
