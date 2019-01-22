/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.interfaces;

import io.strimzi.test.timemeasuring.Operation;
import io.strimzi.test.timemeasuring.TimeMeasuringSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.Map;

@ExtendWith(ExtensionContextParameterResolver.class)
public interface TestSeparator {
    Logger LOGGER = LogManager.getLogger(TestSeparator.class);
    String SEPARATOR_CHAR = "#";

    static void printThreadDump() {
        Map<Thread, StackTraceElement[]> allThreads = Thread.getAllStackTraces();
        for (Thread thread : allThreads.keySet()) {
            StringBuilder sb = new StringBuilder();
            Thread key = thread;
            StackTraceElement[] trace = allThreads.get(key);
            sb.append(key).append("\r\n");
            for (StackTraceElement aTrace : trace) {
                sb.append(" ").append(aTrace).append("\r\n");
            }
            LOGGER.error(sb.toString());
        }
    }

    @BeforeEach
    default void beforeEachTest(TestInfo testInfo) {
        TimeMeasuringSystem.setTestName(testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName());
        TimeMeasuringSystem.startOperation(Operation.TEST_EXECUTION);
        LOGGER.info(String.join("", Collections.nCopies(100, SEPARATOR_CHAR)));
        LOGGER.info(String.format("%s.%s-STARTED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
    }

    @AfterEach
    default void afterEachTest(TestInfo testInfo, ExtensionContext context) {
        TimeMeasuringSystem.stopOperation(Operation.TEST_EXECUTION);
        if (context.getExecutionException().isPresent()) { // on failed
            Throwable ex = context.getExecutionException().get();
            if (ex instanceof OutOfMemoryError) {
                LOGGER.error("Got OOM, dumping thread info");
                printThreadDump();
            } else {
                LOGGER.error("Caught exception {}", ex);
            }
        }
        LOGGER.info(String.format("%s.%s-FINISHED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
        LOGGER.info(String.join("", Collections.nCopies(100, SEPARATOR_CHAR)));
    }
}