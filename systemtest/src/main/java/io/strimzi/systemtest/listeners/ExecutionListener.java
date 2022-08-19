/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.listeners;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.engine.TestTag;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExecutionListener implements TestExecutionListener {
    private static final Logger LOGGER = LogManager.getLogger(TestExecutionListener.class);

    private static List<String> testSuitesNamesToExecute;
    /* read only */ private static TestPlan testPlan;

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public void testPlanExecutionStarted(TestPlan plan) {
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        LOGGER.info("                        Test run started");
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        testPlan = plan;
        printSelectedTestClasses(testPlan);
        testSuitesNamesToExecute = new ArrayList<>(testPlan.getChildren(testPlan.getRoots().toArray(new TestIdentifier[0])[0])).stream()
            .map(testIdentifier -> testIdentifier.getDisplayName()).collect(Collectors.toList());
    }

    public void testPlanExecutionFinished(TestPlan testPlan) {
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        LOGGER.info("                        Test run finished");
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
    }

    private void printSelectedTestClasses(TestPlan plan) {
        LOGGER.info("Following testclasses are selected for run:");
        Arrays.asList(plan.getChildren(plan.getRoots()
                .toArray(new TestIdentifier[0])[0])
                .toArray(new TestIdentifier[0])).forEach(testIdentifier -> LOGGER.info("-> {}", testIdentifier.getLegacyReportingName()));
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
    }

    public static boolean isLastSuite(final ExtensionContext extensionContext) {
        return extensionContext.getRequiredTestClass().getSimpleName().equals(testSuitesNamesToExecute.get(testSuitesNamesToExecute.size() - 1));
    }

    /**
     * Auxiliary method, which checks is next suite from the current is @IsolatedSuite. Next suite @code{followingTestSuite}
     * is obtained by fetching index number from @code{testSuitesNamesToExecute} and then incrementing to one if it is not
     * the last test suite to execute checked by {@link #isLastSuite(ExtensionContext)}.
     *
     * Deep reason of this method, is that when JUnit5 schedule test suites in this order:
     *
     *       @IsolatedSuite -> @IsolatedSuite -> @IsolatedSuite
     *
     * Here we  use {@link SetupClusterOperator#rollbackToDefaultConfiguration()} to rollback to default
     * Cluster Operator configuration. If this worst case when there is no {@link io.strimzi.systemtest.annotations.ParallelSuite}
     * it ends up with:
     *  ------
     *  1) first @IsolatedSuite
     *      a) Deploy Cluster Operator with specific configuration (@BeforeAll)
     *      b) Delete Cluster Operator with specific configuration (@AfterAll)
     *      c) Create Cluster Operator with default configuration (@AfterAll) <- unnecessary
     *  2) second @IsolatedSuite
     *      a) Delete CLuster Operator with default configuration (@BeforeAll) <- unnecessary
     *      b) Create Cluster Operator with specific configuration (@BeforeAll)
     *      c) Delete Cluster Operator with specific configuration (@AfterAll)
     *      d) Create Cluster Operator with default configuration (@AfterAll) <- unnecessary
     *      ...
     *  3) third @IsolatedSuite
     *      a) Delete CLuster Operator with default configuration (@BeforeAll) <- unnecessary
     *     ...
     *
     * @param extensionContext extension context of current test suite, which is executed
     * @return true iff next suite is @IsolatedSuite, otherwise false
     */
    public static boolean isNextSuiteIsolated(final ExtensionContext extensionContext) {
        final int currentTestSuite = testSuitesNamesToExecute.indexOf(extensionContext.getRequiredTestClass().getSimpleName());
        final int followingTestSuite = currentTestSuite + 1;

        if (testSuitesNamesToExecute.size() > 1 && !isLastSuite(extensionContext)) {
            final String followingTestSuiteName = testSuitesNamesToExecute.get(followingTestSuite);
            if (followingTestSuiteName.endsWith(Constants.ST) && testSuitesNamesToExecute.get(followingTestSuite).contains(Constants.ISOLATED)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if test suite has test case, which is labeled as {@link io.strimzi.systemtest.annotations.ParallelTest} or
     * {@link io.strimzi.systemtest.annotations.IsolatedTest}. We check such condition only if we run {@link io.strimzi.systemtest.annotations.ParallelSuite}
     * and thus we create additional namespace only if test suite contains these type of test cases.
     *
     * @param extensionContext  ExtensionContext of the test case
     * @return                  true if test suite contains Parallel or Isolated test case. Otherwise, false.
     */
    public static boolean hasSuiteParallelOrIsolatedTest(final ExtensionContext extensionContext) {
        Set<TestIdentifier> testCases = testPlan.getChildren(extensionContext.getUniqueId());

        for (TestIdentifier testIdentifier : testCases) {
            for (TestTag testTag : testIdentifier.getTags()) {
                if (testTag.getName().equals(Constants.PARALLEL_TEST) || testTag.getName().equals(Constants.ISOLATED_TEST) ||
                    // Dynamic configuration also because in DynamicConfSharedST we use @TestFactory
                    testTag.getName().equals(Constants.DYNAMIC_CONFIGURATION) ||
                    // Tracing, because we deploy Jaeger operator inside additinal namespace
                    testTag.getName().equals(Constants.TRACING)) {
                    return true;
                }
            }
        }
        return false;
    }
}
