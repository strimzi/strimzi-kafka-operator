/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.listeners;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.systemtest.TestTags;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.engine.TestTag;
import org.junit.platform.engine.UniqueId;
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
     * {@link io.strimzi.systemtest.annotations.IsolatedTest} or is suite with explicit need of creating shared namespace.
     *
     * @param extensionContext  ExtensionContext of the test case
     * @return                  true if test suite contains Parallel or Isolated test case. Otherwise, false.
     */
    public static boolean requiresSharedNamespace(final ExtensionContext extensionContext) {
        Set<TestIdentifier> testCases = testPlan.getChildren(UniqueId.parse(extensionContext.getUniqueId()));

        // name of suites or tags which indicates need of creation of shared namespace test-suite-namespace
        final Set<String> identifiersRequiringSharedNamespace = Set.of(
            TestTags.PARALLEL_TEST,
            TestTags.ISOLATED_TEST,
            TestTags.DYNAMIC_CONFIGURATION, // Dynamic configuration also because in DynamicConfSharedST we use @TestFactory
            TestTags.TRACING,  // Tracing, because we deploy Jaeger operator inside additional namespace
            TestTags.KAFKA_SMOKE, // KafkaVersionsST, MigrationST because here we use @ParameterizedTest
            TestTags.KRAFT_UPGRADE
        );

        for (TestIdentifier testIdentifier : testCases) {
            for (TestTag testTag : testIdentifier.getTags()) {
                if (identifiersRequiringSharedNamespace.contains(testTag.getName())) {
                    return true;
                }
            }
        }
        return false;
    }
}
