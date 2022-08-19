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
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
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
    private static final String ST_TESTS_PATH = TestUtils.USER_PATH + "/src/test/java/io/strimzi/systemtest";
    private static final Function<List<String>, List<String>> SUITE_NAMESPACES = (additionalSuiteNamespaces) -> {
        if (Environment.isNamespaceRbacScope()) {
            // rbac namespace we are gonna use INFRA_NAMESPACE (everything is in sequential execution)
            return Collections.singletonList(Constants.INFRA_NAMESPACE);
        } else {
            return additionalSuiteNamespaces;
        }
    };

    private static AtomicInteger counterOfNamespaces;
    private static TestSuiteNamespaceManager instance;

    private List<String> stParallelSuitesNames;
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

    /**
     * ListStFilesNames list all files, which has "ST.java" suffix. This algorithm does it with O(n) time complexity
     * using tail recursion.
     *
     * @param stFiles if file we add to the list @code{stParallelSuitesNames}, otherwise it's directory and we recursively go deeper
     *             inside tree structure
     */
    private void retrieveAllSystemTestsNames(File stFiles) {
        // adding to the list of all namespaces and also eliminate @IsolatedSuite classes, because these classes could
        // use Constants.INFRA_NAMESPACE for their execution without any auxiliary.
        if (stFiles.getName().endsWith(Constants.ST + ".java") && !stFiles.getName().contains(Constants.ISOLATED)) {
            this.stParallelSuitesNames.add(stFiles.getName());
        }

        File[] children = stFiles.listFiles();
        // children is .*ST.java
        if (children == null) {
            return;
        }
        // call recursively all children (directories)
        for (File child : children) {
            retrieveAllSystemTestsNames(child);
        }
    }

    /**
     * Dynamic generation of all auxiliary @code{mapOfAdditionalNamespaces} for @ParallelSuites using method
     * {@link #retrieveAllSystemTestsNames(File)}}. Execution is decomposed in three steps:
     *  1. Scan all test directory
     *  2. Post-processing of test suite names adding '-' between all capitals.
     *  3. Adding auxiliary namespaces to the map @code{mapOfAdditionalNamespaces}
     */
    private void constructMapOfAdditionalNamespaces() {
        this.mapOfAdditionalNamespaces = new HashMap<>();

        // 1. fetch all @ParallelSuite names
        this.stParallelSuitesNames = new ArrayList<>(27); // eliminate un-necessary re-allocation
        retrieveAllSystemTestsNames(new File(ST_TESTS_PATH));

        // 2. for each suite put generated namespace
        for (final String testSuiteName : this.stParallelSuitesNames) {
            final String testSuiteWithoutJavaSuffix = testSuiteName.split("\\.")[0];

            // post-processing each and append "-"
            final StringBuilder testSuiteNamespace = new StringBuilder();
            for (int i = 0; i < testSuiteWithoutJavaSuffix.length(); i++) {
                char c = testSuiteWithoutJavaSuffix.charAt(i);
                if (Character.isUpperCase(c) && i != 0 && i <= testSuiteWithoutJavaSuffix.length() - 2) {
                    testSuiteNamespace.append("-");
                }
                testSuiteNamespace.append(c);
            }
            this.mapOfAdditionalNamespaces.put(testSuiteWithoutJavaSuffix, SUITE_NAMESPACES.apply(Collections.singletonList(testSuiteNamespace.toString().toLowerCase(Locale.ROOT))));
        }
    }

    public void createAdditionalNamespaces(ExtensionContext extensionContext) {
        final String requiredClassName = extensionContext.getRequiredTestClass().getSimpleName();
        final List<String> namespaces = this.mapOfAdditionalNamespaces.get(requiredClassName);
        final String testSuite = extensionContext.getRequiredTestClass().getName();

        if (namespaces != null) {
            LOGGER.debug("Content of the test suite namespaces map:\n" + this.mapOfAdditionalNamespaces.toString());
            LOGGER.debug("Test suite `" + requiredClassName + "` creates these additional namespaces:" + namespaces.toString());

            for (String namespaceName : namespaces) {
                if (namespaceName.equals(Constants.INFRA_NAMESPACE)) {
                    continue;
                }

                if (ExecutionListener.hasSuiteParallelOrIsolatedTest(extensionContext)) {
                    KubeClusterResource.getInstance().createNamespace(CollectorElement.createCollectorElement(testSuite), namespaceName);
                    NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, Collections.singletonList(namespaceName));
                    if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
                        StUtils.copyImagePullSecret(namespaceName);
                    }
                } else {
                    LOGGER.info("We are not gonna create additional namespace: {}, because test suite: {} does not " +
                        "contains @ParallelTest or @IsolatedTest.", namespaceName, requiredClassName);
                }
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

        if (KubeClusterResource.getMapWithSuiteNamespaces().get(collectorElement) != null) {
            Set<String> namespacesToDelete = new HashSet<>(KubeClusterResource.getMapWithSuiteNamespaces().get(collectorElement));
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
