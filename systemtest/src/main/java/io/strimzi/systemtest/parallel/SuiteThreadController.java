/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.systemtest.Constants;
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
    private static Integer maxTestSuitesInParallel;
    private static AtomicInteger runningTestSuitesInParallelCount;
    private static AtomicBoolean isOpen;
    private static AtomicBoolean isParallelSuiteReleased;

    public synchronized static SuiteThreadController getInstance() {
        if (instance == null) {
            instance = new SuiteThreadController();
            if (System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME) != null) {
                maxTestSuitesInParallel =  Integer.parseInt((String) System.getProperties().get(JUNIT_PARALLEL_COUNT_PROPERTY_NAME));
            } else {
                LOGGER.error("User did not specify junit.jupiter.execution.parallel.config.fixed.parallelism " +
                    "in junit-platform.properties gonna use default as 1 (sequence mode)");
                maxTestSuitesInParallel = 1;
            }
        }
        return instance;
    }

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    private SuiteThreadController() {
        runningTestSuitesInParallelCount = new AtomicInteger(0);
        isOpen = new AtomicBoolean(false);
        isParallelSuiteReleased = new AtomicBoolean(false);
        waitingTestSuites = new ArrayList<>();
    }

    public synchronized void addParallelSuite(ExtensionContext extensionContext) {
        LOGGER.debug("[{}] - Adding parallel suite: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), extensionContext.getDisplayName());

        runningTestSuitesInParallelCount.set(runningTestSuitesInParallelCount.incrementAndGet());
        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PARALLEL_CLASS_COUNT, runningTestSuitesInParallelCount.get());

        LOGGER.debug("[{}] - Parallel suites count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestSuitesInParallelCount.get());
    }

    public synchronized void removeParallelSuite(ExtensionContext extensionContext) {
        if (extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PARALLEL_CLASS_COUNT) == null) {
            throw new RuntimeException("There is no parallel suite running.");
        } else {
            LOGGER.debug("[{}] - Removing parallel suite: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), extensionContext.getDisplayName());
            runningTestSuitesInParallelCount.set(runningTestSuitesInParallelCount.decrementAndGet());
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PARALLEL_CLASS_COUNT, runningTestSuitesInParallelCount.get());
        }

        LOGGER.debug("[{}] - Parallel suites count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestSuitesInParallelCount.get());
    }

    public boolean waitUntilZeroParallelSuites(ExtensionContext extensionContext) {
        // until more that 0 parallel suites running in parallel 'active waiting'
        boolean preCondition = true;
        // fail counter eliminates possible OS thread execution schedule
        int isZeroParallelSuitesCounter = 0;

        while (preCondition) {
            LOGGER.trace("[{}] - Parallel suites count: {}", StUtils.removePackageName(extensionContext.getRequiredTestClass().getName()), runningTestSuitesInParallelCount.get());
            try {
                Thread.sleep(STARTING_DELAY);
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
     * Synchronise point where {@link io.strimzi.systemtest.annotations.ParallelSuite} end it in situation when
     * Junit5 {@link java.util.concurrent.ForkJoinPool} spawn additional threads, which can exceed limit specified
     * and thus many threads can start execute test suites which could potentially destroy cluster. This is mechanism,
     * which will all additional threads (i.e., not needed) put into waiting room. After one of the
     * {@link io.strimzi.systemtest.annotations.ParallelSuite} is done with execution we release
     * ({@link #notifyParallelSuiteToAllowExecution(ExtensionContext)} one test suite setting {@code isParallelSuiteReleased} flag.
     * This ensures that only one test suite will continue with execution and others will still wait.
     *
     * @param extensionContext Extension context for test suite name
     */
    public void waitUntilAllowedNumberTestSuitesInParallel(ExtensionContext extensionContext) {
        final String testSuiteToWait = extensionContext.getRequiredTestClass().getSimpleName();
        waitingTestSuites.add(testSuiteToWait);

        while (!isRunningAllowedNumberTestSuitesInParallel()) {
            LOGGER.debug("{} is waiting to proceed with execution but current thread exceed maximum " +
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

    public void incrementParallelSuiteCounter() {
        runningTestSuitesInParallelCount.set(runningTestSuitesInParallelCount.incrementAndGet());
    }

    public void decrementCounter() {
        runningTestSuitesInParallelCount.set(runningTestSuitesInParallelCount.decrementAndGet());
    }

    /**
     * Notifies one of the {@link io.strimzi.systemtest.annotations.ParallelSuite} to continue its execution. Specifically
     * {@code waitingTestSuites}, which are waiting because {@link java.util.concurrent.ForkJoinPool} spawns
     * additional threads, which exceed parallelism limit. This ensures that {@link io.strimzi.systemtest.annotations.ParallelSuite}
     * will not deadlock.
     *
     * @param extensionContext extension context for identifying, which test suite notifies.
     */
    public void notifyParallelSuiteToAllowExecution(ExtensionContext extensionContext) {
        LOGGER.info("{} - Notifies waiting test suites:{} to and randomly select one to start execution", extensionContext.getRequiredTestClass().getSimpleName(), waitingTestSuites.toString());
        isParallelSuiteReleased.set(true);
    }

    public void unLockIsolatedSuite() {
        this.isOpen.set(false);
    }
}
