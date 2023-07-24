/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class responsible for synchronization of Parallel and Isolated tests. When ThreadPoolJoin run several tests in parallel, we
 * increment counter @code{runningTestCasesInParallelCount}. If a counter is zero, we can proceed
 * with the IsolatedTest. If the counter is higher than zero, there is always a running ParallelTest.
 */
public class SuiteThreadController {

    private static final long STARTING_DELAY = Duration.ofSeconds(5).toMillis();
    private static final Logger LOGGER = LogManager.getLogger(SuiteThreadController.class);
    private static final String JUNIT_PARALLEL_COUNT_PROPERTY_NAME = "junit.jupiter.execution.parallel.config.fixed.parallelism";

    private static SuiteThreadController instance;
    private static List<String> waitingTestCases;
    private static Integer maxTestSuitesInParallel;
    private static AtomicInteger runningTestCasesInParallelCount;
    private static AtomicBoolean isParallelTestReleased;

    public synchronized static SuiteThreadController getInstance() {
        if (instance == null) {
            instance = new SuiteThreadController();
            if (System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME) != null) {
                maxTestSuitesInParallel =  Integer.parseInt((String) System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME));
                LOGGER.info("Going to execute {} tests in parallel", maxTestSuitesInParallel);
            } else {
                LOGGER.warn("User did not specify junit.jupiter.execution.parallel.config.fixed.parallelism " +
                    "in junit-platform.properties gonna use default as 1 (sequence mode)");
                maxTestSuitesInParallel = 1;
            }
        }
        return instance;
    }

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    private SuiteThreadController() {
        runningTestCasesInParallelCount = new AtomicInteger(0);
        isParallelTestReleased = new AtomicBoolean(false);
        waitingTestCases = new ArrayList<>();
    }

    public void addParallelTest(ExtensionContext extensionContext) {
        LOGGER.debug("[{}] - Adding parallel test: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), extensionContext.getDisplayName());

        runningTestCasesInParallelCount.incrementAndGet();

        LOGGER.debug("[{}] - Parallel test count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestCasesInParallelCount.get());
    }

    public void removeParallelTest(ExtensionContext extensionContext) {
        LOGGER.debug("[{}] - Removing parallel test: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), extensionContext.getDisplayName());

        runningTestCasesInParallelCount.decrementAndGet();

        LOGGER.debug("[{}] - Parallel test count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestCasesInParallelCount.get());
    }

    /**
     * Synchronise point where {@link io.strimzi.test.annotations.ParallelTest} or
     * {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest} end it in situation when Junit5
     * {@link java.util.concurrent.ForkJoinPool} spawn additional threads, which can exceed limit specified
     * and thus many threads can start execute test cases which could potentially destroy cluster. This is mechanism,
     * which will all additional threads (i.e., not needed) put into waiting room. After one of the
     * {@link io.strimzi.test.annotations.ParallelTest} or {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest}
     * is done with execution we release ({@link #notifyParallelTestToAllowExecution(ExtensionContext)} one test case
     * setting {@code isParallelTestReleased} flag. This ensures that only one test suite will continue with execution
     * and others will still wait.
     *
     * @param extensionContext Extension context for test suite name
     */
    public void waitUntilAllowedNumberTestCasesParallel(ExtensionContext extensionContext) {
        final String testCaseToWait = extensionContext.getDisplayName();
        waitingTestCases.add(testCaseToWait);

        if (runningTestCasesInParallelCount.get() > maxTestSuitesInParallel) {
            LOGGER.debug("[{}] moved to the WaitZone, because current thread exceed maximum of allowed " +
                    "test cases in parallel. ({}/{})", testCaseToWait, runningTestCasesInParallelCount.get(),
                maxTestSuitesInParallel);
        }
        while (!isRunningAllowedNumberTestInParallel()) {
            LOGGER.trace("{} is waiting to proceed with execution but current thread exceed maximum " +
                    "of allowed test cases in parallel. ({}/{})",
                testCaseToWait,
                runningTestCasesInParallelCount.get(),
                maxTestSuitesInParallel);

            try {
                Thread.currentThread().sleep(STARTING_DELAY);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // release and lock again
            if (isParallelTestReleased.get()) {
                // lock
                isParallelTestReleased.set(false);
                waitingTestCases.remove(testCaseToWait);
                break;
            }
        }
        LOGGER.debug("{} test now can proceed its execution", testCaseToWait);
    }

    public boolean isRunningAllowedNumberTestInParallel() {
        return runningTestCasesInParallelCount.get() <= maxTestSuitesInParallel;
    }

    /**
     * Notifies one of the {@link io.strimzi.systemtest.annotations.ParallelTest} or {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest}
     * to continue its execution. Specifically {@code waitingTestCases}, which are waiting because {@link java.util.concurrent.ForkJoinPool}
     * spawns additional threads, which exceed parallelism limit. This ensures that {@link io.strimzi.systemtest.annotations.ParallelTest}
     * or {@link io.strimzi.systemtest.annotations.ParallelNamespaceTest} won't cause deadlock.
     *
     * @param extensionContext extension context for identifying, which test suite notifies.
     */
    public void notifyParallelTestToAllowExecution(ExtensionContext extensionContext) {
        LOGGER.debug("{} - Notifies waiting TestCases: {} to and randomly select one to start execution", extensionContext.getDisplayName(), waitingTestCases.toString());
        isParallelTestReleased.set(true);
    }
}
