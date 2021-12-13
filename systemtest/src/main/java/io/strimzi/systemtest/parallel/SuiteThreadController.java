/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.systemtest.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
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

    private static SuiteThreadController instance;

    private AtomicInteger runningTestSuitesInParallelCount;
    private AtomicBoolean isOpen;

    public synchronized static SuiteThreadController getInstance() {
        if (instance == null) {
            instance = new SuiteThreadController();
        }
        return instance;
    }

    public SuiteThreadController() {

        this.runningTestSuitesInParallelCount = new AtomicInteger(0);
        this.isOpen = new AtomicBoolean(false);
    }

    public synchronized void addParallelSuite(ExtensionContext extensionContext) {
        LOGGER.debug("{} - Adding parallel suite: {}", extensionContext.getRequiredTestClass().getSimpleName(), extensionContext.getDisplayName());

        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PARALLEL_CLASS_COUNT, runningTestSuitesInParallelCount.incrementAndGet());

        LOGGER.debug("{} - Parallel suites count: {}", extensionContext.getRequiredTestClass().getSimpleName(), runningTestSuitesInParallelCount.get());
    }

    public synchronized void removeParallelSuite(ExtensionContext extensionContext) {
        if (extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PARALLEL_CLASS_COUNT) == null) {
            throw new RuntimeException("There is no parallel suite running.");
        } else {
            LOGGER.debug("{} - Removing parallel suite: {}", extensionContext.getRequiredTestClass().getSimpleName(), extensionContext.getDisplayName());
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PARALLEL_CLASS_COUNT, runningTestSuitesInParallelCount.decrementAndGet());
        }

        LOGGER.debug("{} - Parallel suites count: {}", extensionContext.getRequiredTestClass().getSimpleName(), runningTestSuitesInParallelCount.get());
    }

    public boolean waitUntilZeroParallelSuites(ExtensionContext extensionContext) {
        // until more that 0 parallel suites running in parallel 'active waiting'
        boolean preCondition = true;
        // fail counter eliminates possible OS thread execution schedule
        int isZeroParallelSuitesCounter = 0;

        while (preCondition) {
            LOGGER.debug("{} - Parallel suites count: {}", extensionContext.getRequiredTestClass().getSimpleName(), runningTestSuitesInParallelCount.get());
            try {
                Thread.sleep(STARTING_DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            isZeroParallelSuitesCounter = runningTestSuitesInParallelCount.get() <= 0 ? ++isZeroParallelSuitesCounter : 0;

            LOGGER.debug("{} - isZeroParallelSuitesCounter counter is:{}", extensionContext.getRequiredTestClass().getSimpleName(), isZeroParallelSuitesCounter);

            preCondition = runningTestSuitesInParallelCount.get() > 0 || isZeroParallelSuitesCounter < 5;
        }
        return false;
    }

    @SuppressFBWarnings(value = "SWL_SLEEP_WITH_LOCK_HELD")
    public synchronized void waitUntilEntryIsOpen(ExtensionContext extensionContext) {
        // other threads must wait until is open
        while (this.isOpen.get()) {
            LOGGER.debug("{} is waiting to lock to be released.", extensionContext.getRequiredTestClass().getSimpleName());
            try {
                Thread.currentThread().sleep(STARTING_DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // lock flag
        this.isOpen.set(true);
        LOGGER.info("{} has locked the @IsolatedSuite and other @IsolatedSuites must wait until lock is released.", extensionContext.getRequiredTestClass().getSimpleName());
    }

    public void decrementCounter() {
        runningTestSuitesInParallelCount.decrementAndGet();
    }

    public void unLockIsolatedSuite() {
        this.isOpen.set(false);
    }
}
