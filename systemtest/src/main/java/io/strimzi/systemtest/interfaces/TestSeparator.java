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

import java.util.Collections;

@ExtendWith(ExtensionContextParameterResolver.class)
public interface TestSeparator {
    Logger LOGGER = LogManager.getLogger(TestSeparator.class);
    String SEPARATOR_CHAR = "#";

    @BeforeEach
    default void beforeEachTest(TestInfo testInfo) {
        TimeMeasuringSystem.setTestName(testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName());
        TimeMeasuringSystem.startOperation(Operation.TEST_EXECUTION);
        LOGGER.info(String.join("", Collections.nCopies(76, SEPARATOR_CHAR)));
        LOGGER.info(String.format("%s.%s-STARTED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
    }

    @AfterEach
    default void afterEachTest(TestInfo testInfo) {
        TimeMeasuringSystem.stopOperation(Operation.TEST_EXECUTION);
        LOGGER.info(String.format("%s.%s-FINISHED", testInfo.getTestClass().get().getName(), testInfo.getTestMethod().get().getName()));
        LOGGER.info(String.join("", Collections.nCopies(76, SEPARATOR_CHAR)));
    }
}