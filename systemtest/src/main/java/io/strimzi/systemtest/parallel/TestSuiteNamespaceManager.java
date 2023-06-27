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
                LOGGER.info("Creating Namespace: {} for TestCase: {}", namespaceTestCase, testCase);

                KubeClusterResource.getInstance().createNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName(), extensionContext.getRequiredTestMethod().getName()), namespaceTestCase);
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

                LOGGER.info("Deleting Namespace: {} for TestCase: {}", namespaceToDelete, extensionContext.getDisplayName());
                KubeClusterResource.getInstance().deleteNamespace(CollectorElement.createCollectorElement(extensionContext.getRequiredTestClass().getName(), extensionContext.getRequiredTestMethod().getName()), namespaceToDelete);
            }
        }
    }
}
