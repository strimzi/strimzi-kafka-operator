/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.listeners;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.util.Arrays;

public class ExecutionListener implements TestExecutionListener {
    private static final Logger LOGGER = LogManager.getLogger(TestExecutionListener.class);

    private static String lastTestSuiteToExecute;

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public void testPlanExecutionStarted(TestPlan plan) {
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        LOGGER.info("                        Test run started");
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        printSelectedTestClasses(plan);
        Object[] testSuitesToExecute = plan.getChildren(plan.getRoots().toArray(new TestIdentifier[0])[0]).toArray();
        if (testSuitesToExecute != null && testSuitesToExecute.length > 0) {
            lastTestSuiteToExecute = ((TestIdentifier) testSuitesToExecute[testSuitesToExecute.length - 1]).getDisplayName();
        }
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

    public static boolean isLastSuite(ExtensionContext extensionContext) {
        return extensionContext.getRequiredTestClass().getSimpleName().equals(lastTestSuiteToExecute);
    }
}
