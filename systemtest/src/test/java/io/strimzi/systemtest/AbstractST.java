/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Namespace;
import io.skodjob.testframe.resources.BuildConfigType;
import io.skodjob.testframe.resources.ClusterRoleBindingType;
import io.skodjob.testframe.resources.ClusterRoleType;
import io.skodjob.testframe.resources.ConfigMapType;
import io.skodjob.testframe.resources.CustomResourceDefinitionType;
import io.skodjob.testframe.resources.DeploymentType;
import io.skodjob.testframe.resources.ImageStreamType;
import io.skodjob.testframe.resources.JobType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.resources.LeaseType;
import io.skodjob.testframe.resources.NetworkPolicyType;
import io.skodjob.testframe.resources.OperatorGroupType;
import io.skodjob.testframe.resources.RoleBindingType;
import io.skodjob.testframe.resources.RoleType;
import io.skodjob.testframe.resources.SecretType;
import io.skodjob.testframe.resources.ServiceAccountType;
import io.skodjob.testframe.resources.ServiceType;
import io.skodjob.testframe.resources.SubscriptionType;
import io.skodjob.testframe.resources.ValidatingWebhookConfigurationType;
import io.skodjob.testframe.utils.KubeUtils;
import io.strimzi.systemtest.exceptions.KubernetesClusterUnstableException;
import io.strimzi.systemtest.interfaces.IndicativeSentences;
import io.strimzi.systemtest.logs.TestExecutionWatcher;
import io.strimzi.systemtest.parallel.SuiteThreadController;
import io.strimzi.systemtest.parallel.TestSuiteNamespaceManager;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.resources.types.KafkaAccessType;
import io.strimzi.systemtest.resources.types.KafkaBridgeType;
import io.strimzi.systemtest.resources.types.KafkaConnectType;
import io.strimzi.systemtest.resources.types.KafkaConnectorType;
import io.strimzi.systemtest.resources.types.KafkaMirrorMaker2Type;
import io.strimzi.systemtest.resources.types.KafkaNodePoolType;
import io.strimzi.systemtest.resources.types.KafkaRebalanceType;
import io.strimzi.systemtest.resources.types.KafkaTopicType;
import io.strimzi.systemtest.resources.types.KafkaType;
import io.strimzi.systemtest.resources.types.KafkaUserType;
import io.strimzi.systemtest.resources.types.NamespaceType;
import io.strimzi.systemtest.resources.types.StrimziPodSetType;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.interfaces.TestSeparator;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
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

import static io.strimzi.systemtest.matchers.Matchers.logHasNoUnexpectedErrors;
import static org.hamcrest.MatcherAssert.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith({TestExecutionWatcher.class})
@DisplayNameGeneration(IndicativeSentences.class)
@io.skodjob.testframe.annotations.ResourceManager
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public abstract class AbstractST implements TestSeparator {
    public static final List<String> LB_FINALIZERS;
    static {
        LB_FINALIZERS = Environment.LB_FINALIZERS ? List.of(TestConstants.LOAD_BALANCER_CLEANUP) : null;

        KubeResourceManager.get().setResourceTypes(
            new ClusterRoleBindingType(),
            new ClusterRoleType(),
            new CustomResourceDefinitionType(),
            new DeploymentType(),
            new NamespaceType(),
            new JobType(),
            new NetworkPolicyType(),
            new RoleBindingType(),
            new ServiceType(),
            new ConfigMapType(),
            new LeaseType(),
            new ServiceAccountType(),
            new RoleType(),
            new SecretType(),
            new ValidatingWebhookConfigurationType(),
            new SubscriptionType(),
            new OperatorGroupType(),
            new BuildConfigType(),
            new ImageStreamType(),
            new KafkaAccessType(),
            new KafkaBridgeType(),
            new KafkaConnectorType(),
            new KafkaConnectType(),
            new KafkaMirrorMaker2Type(),
            new KafkaNodePoolType(),
            new KafkaRebalanceType(),
            new KafkaTopicType(),
            new KafkaType(),
            new KafkaUserType(),
            new StrimziPodSetType()
        );

        KubeResourceManager.get().addCreateCallback(resource -> {
            if (resource instanceof Namespace namespace) {
                String testClass = StUtils.removePackageName(KubeResourceManager.get().getTestContext().getRequiredTestClass().getName());

                KubeUtils.labelNamespace(
                    namespace.getMetadata().getName(),
                    TestConstants.TEST_SUITE_NAME_LABEL,
                    testClass
                );

                if (KubeResourceManager.get().getTestContext().getTestMethod().isPresent()) {
                    String testCaseName = KubeResourceManager.get().getTestContext().getRequiredTestMethod().getName();

                    KubeUtils.labelNamespace(
                        namespace.getMetadata().getName(),
                        TestConstants.TEST_CASE_NAME_LABEL,
                        StUtils.trimTestCaseBaseOnItsLength(testCaseName)
                    );
                }
            }
        });
    }

    // Test-Frame integration stuff, remove everything else when not needed
    protected final TestSuiteNamespaceManager testSuiteNamespaceManager = TestSuiteNamespaceManager.getInstance();
    private final SuiteThreadController parallelSuiteController = SuiteThreadController.getInstance();
    protected KubeClusterResource cluster;
    private static final Logger LOGGER = LogManager.getLogger(AbstractST.class);

    // {thread-safe} this needs to be static because when more threads spawns diff. TestSuites it might produce race conditions
    private static final Object LOCK = new Object();

    protected void assertNoCoErrorsLogged(String namespaceName, long sinceSeconds) {
        LOGGER.info("Search in strimzi-cluster-operator log for errors in last {} second(s)", sinceSeconds);
        String clusterOperatorLog = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).searchInLog(TestConstants.DEPLOYMENT, SetupClusterOperator.getInstance().getOperatorDeploymentName(), sinceSeconds, "Exception", "Error", "Throwable", "OOM");
        assertThat(clusterOperatorLog, logHasNoUnexpectedErrors());
    }

    private void afterEachMustExecute() {
        if (cluster.cluster().isClusterUp()) {
            if (StUtils.isParallelTest(KubeResourceManager.get().getTestContext()) ||
                StUtils.isParallelNamespaceTest(KubeResourceManager.get().getTestContext())) {
                parallelSuiteController.notifyParallelTestToAllowExecution(KubeResourceManager.get().getTestContext());
                parallelSuiteController.removeParallelTest(KubeResourceManager.get().getTestContext());
            }
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    protected void afterEachMayOverride() {
        if (!Environment.SKIP_TEARDOWN) {
            KubeResourceManager.get().deleteResources();
        }
    }

    private void afterAllMustExecute()  {
        if (!cluster.cluster().isClusterUp()) {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    protected synchronized void afterAllMayOverride() {
        if (!Environment.SKIP_TEARDOWN) {
            KubeResourceManager.get().deleteResources();
            testSuiteNamespaceManager.deleteTestSuiteNamespace();
        }
    }

    /**
     * BeforeEachMayOverride, is a method, which gives you option to override @BeforeAll in sub-classes and
     * ensure that this is also executed if you call it with super.beforeEachMayOverride(). You can also skip it and
     * you your implementation in sub-class as you want.
     */
    protected void beforeEachMayOverride() {
        // this is because we need to have different clusterName and kafkaClientsName in each test case without
        // synchronization it can produce `data-race`
        synchronized (LOCK) {
            LOGGER.info("Not first test we are gonna generate cluster name");
            testSuiteNamespaceManager.createParallelNamespace();
        }
    }

    private void beforeEachMustExecute() {
        if (cluster.cluster().isClusterUp()) {
            if (StUtils.isParallelNamespaceTest(KubeResourceManager.get().getTestContext()) ||
                StUtils.isParallelTest(KubeResourceManager.get().getTestContext())) {
                parallelSuiteController.addParallelTest(KubeResourceManager.get().getTestContext());
                parallelSuiteController.waitUntilAllowedNumberTestCasesParallel(KubeResourceManager.get().getTestContext());
            }
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    private void beforeAllMustExecute() {
        if (cluster.cluster().isClusterUp()) {
        } else {
            throw new KubernetesClusterUnstableException("Cluster is not responding and its probably un-stable (i.e., caused by network, OOM problem)");
        }
    }

    /**
     * BeforeAllMayOverride, is a method, which gives you option to override @BeforeAll in sub-classes and
     * ensure that this is also executed if you call it with super.beforeAllMayOverride(). You can also skip it and
     * you your implementation in sub-class as you want.
     */
    protected void beforeAllMayOverride() {
        cluster = KubeClusterResource.getInstance();
        testSuiteNamespaceManager.createTestSuiteNamespace();
    }

    @BeforeEach
    void setUpTestCase(ExtensionContext extensionContext) {
        // Check if we execute tests in parallel to update logger appender dynamically
        boolean parallelEnabled = Boolean.getBoolean("junit.jupiter.execution.parallel.enabled");
        if (extensionContext.getTestClass().isPresent() && parallelEnabled) {
            ThreadContext.put("testClass", extensionContext.getTestClass().get().getSimpleName());
        }
        if (extensionContext.getTestMethod().isPresent() && parallelEnabled) {
            ThreadContext.put("testMethod", extensionContext.getTestMethod().get().getName());
        }
        KubeResourceManager.get().setTestContext(extensionContext);
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("————————————  {}@Before Each - Setup TestCase environment ———————————— ", StUtils.removePackageName(this.getClass().getName()));
        beforeEachMustExecute();
        beforeEachMayOverride();
    }

    @BeforeAll
    void setUpTestSuite(ExtensionContext extensionContext) {
        // Check if we execute tests in parallel to update logger appender dynamically
        boolean parallelEnabled = Boolean.getBoolean("junit.jupiter.execution.parallel.enabled");
        if (extensionContext.getTestClass().isPresent() && parallelEnabled) {
            ThreadContext.put("testClass", extensionContext.getTestClass().get().getSimpleName());
        }
        KubeResourceManager.get().setTestContext(extensionContext);
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@Before All - Setup TestSuite environment ———————————— ", StUtils.removePackageName(this.getClass().getName()));
        beforeAllMayOverride();
        beforeAllMustExecute();
    }

    @AfterEach
    void tearDownTestCase(ExtensionContext extensionContext) throws Exception {
        KubeResourceManager.get().setTestContext(extensionContext);
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@After Each - Clean up after test ————————————", StUtils.removePackageName(this.getClass().getName()));
        // try with finally is needed because in worst case possible if the Cluster is unable to delete namespaces, which
        // results in `Timeout after 480000 ms waiting for Namespace namespace-136 removal` it throws WaitException and
        // does not proceed with the next method (i.e., afterEachMustExecute()). This ensures that if such problem happen
        // it will always execute the second method.
        try {
            // This method needs to be disabled for the moment, as it brings flakiness and is unstable due to regexes and current matcher checks.
            // Needs to be reworked on what errors to ignore. Better error logging should be added.
//            assertNoCoErrorsLogged(SetupClusterOperator.getInstance().getOperatorNamespace(), (long) extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.TEST_EXECUTION_START_TIME_KEY));
        } finally {
            afterEachMayOverride();
            afterEachMustExecute();
            ThreadContext.remove("testMethod");
        }
    }

    @AfterAll
    void tearDownTestSuite(ExtensionContext extensionContext) {
        KubeResourceManager.get().setTestContext(extensionContext);
        LOGGER.debug(String.join("", Collections.nCopies(76, "=")));
        LOGGER.debug("———————————— {}@After All - Clean up after TestSuite ———————————— ", StUtils.removePackageName(this.getClass().getName()));
        afterAllMayOverride();
        afterAllMustExecute();
        ThreadContext.remove("testClass");
    }
}
