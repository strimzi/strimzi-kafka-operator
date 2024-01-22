/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.systemtest.exceptions.KubernetesClusterUnstableException;
import io.strimzi.systemtest.interfaces.IndicativeSentences;
import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.parallel.SuiteThreadController;
import io.strimzi.systemtest.parallel.TestSuiteNamespaceManager;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.interfaces.TestSeparator;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static io.strimzi.systemtest.matchers.Matchers.logHasNoUnexpectedErrors;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({TestExecutionWatcher.class})
@DisplayNameGeneration(IndicativeSentences.class)
public abstract class AbstractST implements TestSeparator {
    public static final List<String> LB_FINALIZERS;
    static {
        LB_FINALIZERS = Environment.LB_FINALIZERS ? List.of(TestConstants.LOAD_BALANCER_CLEANUP) : null;
    }

    protected final ResourceManager resourceManager = ResourceManager.getInstance();
    protected final TestSuiteNamespaceManager testSuiteNamespaceManager = TestSuiteNamespaceManager.getInstance();
    private final SuiteThreadController parallelSuiteController = SuiteThreadController.getInstance();
    protected SetupClusterOperator clusterOperator = SetupClusterOperator.getInstance();
    protected KubeClusterResource cluster;
    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);

    // {thread-safe} this needs to be static because when more threads spawns diff. TestSuites it might produce race conditions
    private static final Object LOCK = new Object();

    protected static ConcurrentHashMap<ExtensionContext, TestStorage> storageMap = new ConcurrentHashMap<>();

    private void afterEachMustExecute(ExtensionContext extensionContext) {
        if (cluster.cluster().isClusterUp()) {
            if (StUtils.isParallelTest(extensionContext) ||
                StUtils.isParallelNamespaceTest(extensionContext)) {
                parallelSuiteController.notifyParallelTestToAllowExecution(extensionContext);
                parallelSuiteController.removeParallelTest(extensionContext);
            }
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    protected void afterEachMayOverride(ExtensionContext extensionContext) throws Exception {
        if (!Environment.SKIP_TEARDOWN) {
            ResourceManager.getInstance().deleteResources(extensionContext);
            testSuiteNamespaceManager.deleteParallelNamespace(extensionContext);
        }
    }

    private void afterAllMustExecute(ExtensionContext extensionContext)  {
        if (cluster.cluster().isClusterUp()) {
            clusterOperator = SetupClusterOperator.getInstance();
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    protected synchronized void afterAllMayOverride(ExtensionContext extensionContext) {
        if (!Environment.SKIP_TEARDOWN) {
            ResourceManager.getInstance().deleteResources(extensionContext);
            testSuiteNamespaceManager.deleteTestSuiteNamespace(extensionContext);
            NamespaceManager.getInstance().deleteAllNamespacesFromSet();
        }
    }

    /**
     * BeforeEachMayOverride, is a method, which gives you option to override @BeforeAll in sub-classes and
     * ensure that this is also executed if you call it with super.beforeEachMayOverride(). You can also skip it and
     * you your implementation in sub-class as you want.
     * @param extensionContext
     */
    protected void beforeEachMayOverride(ExtensionContext extensionContext) {
        // this is because we need to have different clusterName and kafkaClientsName in each test case without
        // synchronization it can produce `data-race`
        synchronized (LOCK) {
            LOGGER.info("Not first test we are gonna generate cluster name");
            testSuiteNamespaceManager.createParallelNamespace(extensionContext);
            storageMap.put(extensionContext, new TestStorage(extensionContext));
        }
    }

    private void beforeEachMustExecute(ExtensionContext extensionContext) {
        if (cluster.cluster().isClusterUp()) {
            if (StUtils.isParallelNamespaceTest(extensionContext) ||
                StUtils.isParallelTest(extensionContext)) {
                parallelSuiteController.addParallelTest(extensionContext);
                parallelSuiteController.waitUntilAllowedNumberTestCasesParallel(extensionContext);
            }
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    private void beforeAllMustExecute(ExtensionContext extensionContext) {
        if (cluster.cluster().isClusterUp()) {
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    /**
     * BeforeAllMayOverride, is a method, which gives you option to override @BeforeAll in sub-classes and
     * ensure that this is also executed if you call it with super.beforeAllMayOverride(). You can also skip it and
     * you your implementation in sub-class as you want.
     * @param extensionContext
     */
    protected void beforeAllMayOverride(ExtensionContext extensionContext) {
        cluster = KubeClusterResource.getInstance();
        testSuiteNamespaceManager.createTestSuiteNamespace(extensionContext);
    }

    @BeforeEach
    void setUpTestCase(ExtensionContext extensionContext) {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("————————————  {}@Before Each - Setup TestCase environment ———————————— ", StUtils.removePackageName(this.getClass().getName()));
        beforeEachMustExecute(extensionContext);
        beforeEachMayOverride(extensionContext);
    }

    @BeforeAll
    void setUpTestSuite(ExtensionContext extensionContext) {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@Before All - Setup TestSuite environment ———————————— ", StUtils.removePackageName(this.getClass().getName()));
        beforeAllMayOverride(extensionContext);
        beforeAllMustExecute(extensionContext);
    }

    @AfterEach
    void tearDownTestCase(ExtensionContext extensionContext) throws Exception {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@After Each - Clean up after test ————————————", StUtils.removePackageName(this.getClass().getName()));
        // try with finally is needed because in worst case possible if the Cluster is unable to delete namespaces, which
        // results in `Timeout after 480000 ms waiting for Namespace namespace-136 removal` it throws WaitException and
        // does not proceed with the next method (i.e., afterEachMustExecute()). This ensures that if such problem happen
        // it will always execute the second method.
        try {
            assertNoCoErrorsLogged(clusterOperator.getDeploymentNamespace(), storageMap.get(extensionContext).getTestExecutionStartTime());
            afterEachMayOverride(extensionContext);
        } finally {
            afterEachMustExecute(extensionContext);
        }
    }

    @AfterAll
    void tearDownTestSuite(ExtensionContext extensionContext) {
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@After All - Clean up after TestSuite ———————————— ", StUtils.removePackageName(this.getClass().getName()));
        afterAllMayOverride(extensionContext);
        afterAllMustExecute(extensionContext);
    }

    public static void assertNoCoErrorsLogged(String namespaceName, long sinceSeconds) {
        LOGGER.info("Search in strimzi-cluster-operator log for errors in last {} second(s)", sinceSeconds);
        String clusterOperatorLog = cmdKubeClient(namespaceName).searchInLog(TestConstants.DEPLOYMENT, ResourceManager.getCoDeploymentName(), sinceSeconds, "Exception", "Error", "Throwable", "OOM");
        assertThat(clusterOperatorLog, logHasNoUnexpectedErrors());
    }
}
