/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.listeners.ExecutionListener;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

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
     * Creates a test suite namespace {@code Environment.TEST_SUITE_NAMESPACE} if conditions are met (e.g., the test class
     * contains {@link io.strimzi.systemtest.annotations.IsolatedTest} or {@link io.strimzi.systemtest.annotations.ParallelTest}
     * test cases we will create such namespace, otherwise not). When we have test class, which only contains
     *  {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest}, which creates its own namespace it does not
     *  make sense to provide another auxiliary namespace (because it will be not used) and thus we are skip such creation.
     *
     */
    public void createTestSuiteNamespace() {
        final String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();

        if (ExecutionListener.hasSuiteParallelOrIsolatedTest(ResourceManager.getTestContext())) {
            // if RBAC is enabled we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                NamespaceManager.getInstance().createNamespaceAndPrepare(Environment.TEST_SUITE_NAMESPACE, CollectorElement.createCollectorElement(testSuiteName));
            } else {
                LOGGER.info("We are not gonna create additional namespace: {}, because test suite: {} does not " +
                        "contains @ParallelTest or @IsolatedTest.", Environment.TEST_SUITE_NAMESPACE, testSuiteName);
            }
        }
    }

    /**
     * Analogically, inverse method to {@link #createTestSuiteNamespace()}.
     *
     */
    public void deleteTestSuiteNamespace() {
        if (ExecutionListener.hasSuiteParallelOrIsolatedTest(ResourceManager.getTestContext())) {
            // if RBAC is enabled we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                final String testSuiteName = ResourceManager.getTestContext().getRequiredTestClass().getName();

                LOGGER.info("Deleting Namespace: {} for TestSuite: {}", Environment.TEST_SUITE_NAMESPACE, StUtils.removePackageName(testSuiteName));
                NamespaceManager.getInstance().deleteNamespaceWithWaitAndRemoveFromSet(Environment.TEST_SUITE_NAMESPACE, CollectorElement.createCollectorElement(testSuiteName));
            }
        }

    }

    /**
     * In test cases, where Kafka cluster is deployed we always create another namespace. Such test case is then
     * annotated as @ParallelNamespaceTest. This method creates from @code{extensionContext} this type of namespace
     * and store it to the @code{KubeClusterResource} instance, which then it will be needed in the @AfterEach phase.
     * The inverse operation to this one is implement in {@link #deleteParallelNamespace()}.
     *
     */
    public void createParallelNamespace() {
        final String testCaseName = ResourceManager.getTestContext().getRequiredTestMethod().getName();

        // if 'parallel namespace test' we are gonna create namespace
        if (StUtils.isParallelNamespaceTest(ResourceManager.getTestContext())) {
            // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                final String namespaceTestCase = "namespace-" + counterOfNamespaces.getAndIncrement();

                ResourceManager.getTestContext().getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.NAMESPACE_KEY, namespaceTestCase);
                // create namespace by
                LOGGER.info("Creating Namespace: {} for TestCase: {}", namespaceTestCase, StUtils.removePackageName(testCaseName));

                NamespaceManager.getInstance().createNamespaceAndPrepare(namespaceTestCase, CollectorElement.createCollectorElement(ResourceManager.getTestContext().getRequiredTestClass().getName(), testCaseName));
            }
        }
    }

    /**
     * Analogically to the {@link #createParallelNamespace()}.
     *
     */
    public void deleteParallelNamespace() {
        // if 'parallel namespace test' we are gonna delete namespace
        if (StUtils.isParallelNamespaceTest(ResourceManager.getTestContext())) {
            // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                final String namespaceToDelete = ResourceManager.getTestContext().getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.NAMESPACE_KEY).toString();
                final String testCaseName = ResourceManager.getTestContext().getRequiredTestMethod().getName();

                LOGGER.info("Deleting Namespace: {} for TestCase: {}", namespaceToDelete, StUtils.removePackageName(testCaseName));

                NamespaceManager.getInstance().deleteNamespaceWithWaitAndRemoveFromSet(namespaceToDelete, CollectorElement.createCollectorElement(ResourceManager.getTestContext().getRequiredTestClass().getName(), testCaseName));
            }
        }
    }
}
