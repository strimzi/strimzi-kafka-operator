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
 * Class responsible for synchronization of Parallel and Isolated suites. When ThreadPoolJoin run several tests in parallel, we
 * increment counter @code{runningTestSuitesInParallelCount} using shared extension context. If a counter is zero, we can proceed
 * with the IsolationSuite. If the counter is higher than zero, there is always a running ParallelSuite. Moreover, we need
 * additional condition for IsolatedSuite, where more than 2 threads wants to execute IsolatedSuite. If this type of scenario
 * one Thread A win for spot and set flag @code{isOpen} to true and Thread B active wait until in @AfterAll thread A release
 * and set flag @code{isOpen} to false.
 */
public class SuiteThreadController {

    private static final long STARTING_DELAY = Duration.ofSeconds(5).toMillis();
    private static final Logger LOGGER = LogManager.getLogger(SuiteThreadController.class);
    private static final String JUNIT_PARALLEL_COUNT_PROPERTY_NAME = "junit.jupiter.execution.parallel.config.fixed.parallelism";

    private static SuiteThreadController instance;
    private static List<String> waitingTestSuites;
    private static List<String> waitingTestCases;
    private static Integer maxTestSuitesInParallel;
    private static AtomicInteger runningTestSuitesInParallelCount;
    private static AtomicInteger runningTestCasesInParallelCount;
    private static AtomicBoolean isOpen;
    private static AtomicBoolean isParallelSuiteReleased;
    private static AtomicBoolean isParallelTestReleased;

    public synchronized static SuiteThreadController getInstance() {
        if (instance == null) {
            instance = new SuiteThreadController();
            if (System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME) != null) {
                maxTestSuitesInParallel =  Integer.parseInt((String) System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME));
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
        runningTestSuitesInParallelCount = new AtomicInteger(0);
        runningTestCasesInParallelCount = new AtomicInteger(0);
        isOpen = new AtomicBoolean(false);
        isParallelSuiteReleased = new AtomicBoolean(false);
        isParallelTestReleased = new AtomicBoolean(false);
        waitingTestSuites = new ArrayList<>();
        waitingTestCases = new ArrayList<>();
    }

    public void addParallelSuite(ExtensionContext extensionContext) {
        LOGGER.debug("[{}] - Adding parallel suite: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), extensionContext.getDisplayName());

        runningTestSuitesInParallelCount.incrementAndGet();

        LOGGER.debug("[{}] - Parallel suites count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestSuitesInParallelCount.get());
    }

    public void addParallelTest(ExtensionContext extensionContext) {
        LOGGER.debug("[{}] - Adding parallel test: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), extensionContext.getDisplayName());

        runningTestCasesInParallelCount.incrementAndGet();

        LOGGER.debug("[{}] - Parallel test count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestCasesInParallelCount.get());
    }

    public void removeParallelSuite(ExtensionContext extensionContext) {
        LOGGER.debug("[{}] - Removing parallel suite: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), extensionContext.getDisplayName());

        runningTestSuitesInParallelCount.decrementAndGet();

        LOGGER.debug("[{}] - Parallel suites count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestSuitesInParallelCount.get());
    }

    public void removeParallelTest(ExtensionContext extensionContext) {
        LOGGER.debug("[{}] - Removing parallel test: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), extensionContext.getDisplayName());

        runningTestCasesInParallelCount.decrementAndGet();

        LOGGER.debug("[{}] - Parallel test count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestCasesInParallelCount.get());
    }

    public boolean waitUntilZeroParallelSuites(ExtensionContext extensionContext) {
        // until more that 0 parallel suites running in parallel 'active waiting'
        boolean preCondition = true;
        // fail counter eliminates possible OS thread execution schedule
        int isZeroParallelSuitesCounter = 0;

        while (preCondition) {
            LOGGER.trace("[{}] - Parallel suites count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestSuitesInParallelCount.get());
            try {
                Thread.currentThread().sleep(STARTING_DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            isZeroParallelSuitesCounter = runningTestSuitesInParallelCount.get() <= 0 ? ++isZeroParallelSuitesCounter : 0;

            LOGGER.trace("[{}] - isZeroParallelSuitesCounter counter is: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), isZeroParallelSuitesCounter);

            preCondition = runningTestSuitesInParallelCount.get() > 0 || isZeroParallelSuitesCounter < 5;
        }
        return false;
    }

    @SuppressFBWarnings(value = "SWL_SLEEP_WITH_LOCK_HELD")
    public synchronized void waitUntilEntryIsOpen(ExtensionContext extensionContext) {
        // other threads must wait until is open
        while (this.isOpen.get()) {
            LOGGER.debug("Suite {} is waiting to lock to be released.", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()));
            try {
                Thread.currentThread().sleep(STARTING_DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // lock flag
        this.isOpen.set(true);
        LOGGER.debug("Suite {} has locked the @IsolatedSuite and other @IsolatedSuites must wait until lock is released.", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()));
    }

    public boolean isRunningAllowedNumberTestSuitesInParallel() {
        return runningTestSuitesInParallelCount.get() <= maxTestSuitesInParallel;
    }

    /**
     * Synchronise point where {@link io.strimzi.test.annotations.ParallelSuite} end it in situation when
     * Junit5 {@link java.util.concurrent.ForkJoinPool} spawn additional threads, which can exceed limit specified
     * and thus many threads can start execute test suites which could potentially destroy cluster. This is mechanism,
     * which will all additional threads (i.e., not needed) put into waiting room. After one of the
     * {@link io.strimzi.test.annotations.ParallelSuite} is done with execution we release
     * ({@link #notifyParallelSuiteToAllowExecution(ExtensionContext)} one test suite setting {@code isParallelSuiteReleased} flag.
     * This ensures that only one test suite will continue with execution and others will still wait.
     *
     * @param extensionContext Extension context for test suite name
     */
    public void waitUntilAllowedNumberTestSuitesInParallel(ExtensionContext extensionContext) {
        final String testSuiteToWait = extensionContext.getRequiredTestClass().getSimpleName();
        waitingTestSuites.add(testSuiteToWait);

        if (runningTestSuitesInParallelCount.get() > maxTestSuitesInParallel) {
            LOGGER.debug("[{}] moved to the WaitZone, because current thread exceed maximum of allowed " +
                    "test suites in parallel. ({}/{})", testSuiteToWait, runningTestSuitesInParallelCount.get(),
                maxTestSuitesInParallel);
        }
        while (!isRunningAllowedNumberTestSuitesInParallel()) {
            LOGGER.trace("{} is waiting to proceed with execution but current thread exceed maximum " +
                "of allowed test suites in parallel. ({}/{})",
                testSuiteToWait,
                runningTestSuitesInParallelCount.get(),
                maxTestSuitesInParallel);

            try {
                Thread.currentThread().sleep(STARTING_DELAY);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // release and lock again
            if (isParallelSuiteReleased.get()) {
                // lock
                isParallelSuiteReleased.set(false);
                waitingTestSuites.remove(testSuiteToWait);
                break;
            }
        }
        LOGGER.debug("{} suite now can proceed its execution", testSuiteToWait);
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

    public void incrementParallelSuiteCounter() {
        runningTestSuitesInParallelCount.set(runningTestSuitesInParallelCount.incrementAndGet());
    }

    public void decrementParallelSuiteCounter() {
        runningTestSuitesInParallelCount.set(runningTestSuitesInParallelCount.decrementAndGet());
    }

    /**
     * Notifies one of the {@link io.strimzi.test.annotations.ParallelSuite} to continue its execution. Specifically
     * {@code waitingTestSuites}, which are waiting because {@link java.util.concurrent.ForkJoinPool} spawns
     * additional threads, which exceed parallelism limit. This ensures that {@link io.strimzi.test.annotations.ParallelSuite}
     * will not deadlock.
     *
     * @param extensionContext extension context for identifying, which test suite notifies.
     */
    public void notifyParallelSuiteToAllowExecution(ExtensionContext extensionContext) {
        LOGGER.debug("{} - Notifies waiting test suites:{} to and randomly select one to start execution", extensionContext.getRequiredTestClass().getSimpleName(), waitingTestSuites.toString());
        isParallelSuiteReleased.set(true);
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
        LOGGER.debug("{} - Notifies waiting test cases:{} to and randomly select one to start execution", extensionContext.getDisplayName(), waitingTestCases.toString());
        isParallelTestReleased.set(true);
    }

    public void unLockIsolatedSuite() {
        this.isOpen.set(false);
    }
}
