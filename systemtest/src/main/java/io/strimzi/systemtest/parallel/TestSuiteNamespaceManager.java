/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.listeners.ExecutionListener;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class contains static information (additional namespaces will are needed for test suite) before the execution of @BeforeAll.
 * By this we can easily prepare namespace in the AbstractST class and not in the the children.
 */
public class TestSuiteNamespaceManager {

    private static final Logger LOGGER = LogManager.getLogger(TestSuiteNamespaceManager.class);

    private static AtomicInteger counterOfNamespaces;
    private static TestSuiteNamespaceManager instance;

    public synchronized static TestSuiteNamespaceManager getInstance() {
        if (instance == null) {
            instance = new TestSuiteNamespaceManager();
            counterOfNamespaces = new AtomicInteger(0);
        }
        return instance;
    }

    /**
     * Creates a test suite namespace {@code Constants.TEST_SUITE_NAMESPACE} if conditions are met (e.g., the test class
     * contains {@link io.strimzi.systemtest.annotations.IsolatedTest} or {@link io.strimzi.systemtest.annotations.ParallelTest}
     * test cases we will create such namespace, otherwise not). When we have test class, which only contains
     *  {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest}, which creates its own namespace it does not
     *  make sense to provide another auxiliary namespace (because it will be not used) and thus we are skip such creation.
     *
     * @param extensionContext      extension context for test class
     */
    public void createTestSuiteNamespace(ExtensionContext extensionContext) {
        final String testSuiteName = extensionContext.getRequiredTestClass().getName();

        if (ExecutionListener.hasSuiteParallelOrIsolatedTest(extensionContext)) {
            // if RBAC is enabled we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                KubeClusterResource.getInstance().createNamespace(CollectorElement.createCollectorElement(testSuiteName), Constants.TEST_SUITE_NAMESPACE);
                NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, Collections.singletonList(Constants.TEST_SUITE_NAMESPACE));
                StUtils.copyImagePullSecrets(Constants.TEST_SUITE_NAMESPACE);
            } else {
                LOGGER.info("We are not gonna create additional namespace: {}, because test suite: {} does not " +
                        "contains @ParallelTest or @IsolatedTest.", Constants.TEST_SUITE_NAMESPACE, testSuiteName);
            }
        }
    }

    /**
     * Analogically, inverse method to {@link #createTestSuiteNamespace(ExtensionContext)}.
     *
     * @param extensionContext      extension context for test class
     */
    public void deleteTestSuiteNamespace(ExtensionContext extensionContext) {
        if (ExecutionListener.hasSuiteParallelOrIsolatedTest(extensionContext)) {
            // if RBAC is enabled we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                final String testSuiteName = extensionContext.getRequiredTestClass().getName();

                LOGGER.info("Deleting Namespace: {} for TestSuite: {}", Constants.TEST_SUITE_NAMESPACE, StUtils.removePackageName(testSuiteName));
                KubeClusterResource.getInstance().deleteNamespace(CollectorElement.createCollectorElement(testSuiteName), Constants.TEST_SUITE_NAMESPACE);
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
        final String testCaseName = extensionContext.getRequiredTestMethod().getName();

        // if 'parallel namespace test' we are gonna create namespace
        if (StUtils.isParallelNamespaceTest(extensionContext)) {
            // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                final String namespaceTestCase = "namespace-" + counterOfNamespaces.getAndIncrement();

                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.NAMESPACE_KEY, namespaceTestCase);
                // create namespace by
                LOGGER.info("Creating Namespace: {} for TestCase: {}", namespaceTestCase, StUtils.removePackageName(testCaseName));

                KubeClusterResource.getInstance().createNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName(), testCaseName), namespaceTestCase);
                NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, Collections.singletonList(namespaceTestCase));
                StUtils.copyImagePullSecrets(namespaceTestCase);
            }
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
                final String testCaseName = extensionContext.getRequiredTestMethod().getName();

                LOGGER.info("Deleting Namespace: {} for TestCase: {}", namespaceToDelete, StUtils.removePackageName(testCaseName));
                KubeClusterResource.getInstance().deleteNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName(), testCaseName), namespaceToDelete);
            }
        }
    }
}
