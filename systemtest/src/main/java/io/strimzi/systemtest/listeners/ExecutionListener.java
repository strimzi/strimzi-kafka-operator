/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.listeners;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.systemtest.TestConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.engine.TestTag;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.util.Arrays;
import java.util.Set;

public class ExecutionListener implements TestExecutionListener {
    private static final Logger LOGGER = LogManager.getLogger(TestExecutionListener.class);

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

    /**
     * Checks if test suite has test case, which is labeled as {@link io.strimzi.systemtest.annotations.ParallelTest} or
     * {@link io.strimzi.systemtest.annotations.IsolatedTest}.
     *
     * @param extensionContext  ExtensionContext of the test case
     * @return                  true if test suite contains Parallel or Isolated test case. Otherwise, false.
     */
    public static boolean hasSuiteParallelOrIsolatedTest(final ExtensionContext extensionContext) {
        Set<TestIdentifier> testCases = testPlan.getChildren(extensionContext.getUniqueId());

        for (TestIdentifier testIdentifier : testCases) {
            for (TestTag testTag : testIdentifier.getTags()) {
                if (testTag.getName().equals(TestConstants.PARALLEL_TEST) || testTag.getName().equals(TestConstants.ISOLATED_TEST) ||
                        // Dynamic configuration also because in DynamicConfSharedST we use @TestFactory
                        testTag.getName().equals(TestConstants.DYNAMIC_CONFIGURATION) ||
                        // Tracing, because we deploy Jaeger operator inside additional namespace
                        testTag.getName().equals(TestConstants.TRACING) ||
                        // KafkaVersionsST, because here we use @ParameterizedTest
                        testTag.getName().equals(TestConstants.KAFKA_SMOKE)) {
                    return true;
                }
            }
        }
        return false;
    }
}
