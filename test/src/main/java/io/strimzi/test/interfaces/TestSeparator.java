/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.interfaces;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;

/**
 * Separates different tests in the log output
 */
@ExtendWith(ExtensionContextParameterResolver.class)
public interface TestSeparator {
    /**
     * Logger used to log the separator message
     */
    Logger LOGGER = LogManager.getLogger(TestSeparator.class);
    /**
     * Separator character used in the log output
     */
    String SEPARATOR_CHAR = "#";

    /**
     * Prints the separator at the start of the test
     *
     * @param testContext   Test context
     */
    @BeforeEach
    default void beforeEachTest(ExtensionContext testContext) {
        LOGGER.info(String.join("", Collections.nCopies(76, SEPARATOR_CHAR)));
        LOGGER.info(String.format("%s.%s-STARTED", testContext.getRequiredTestClass().getName(), testContext.getRequiredTestMethod().getName()));
    }

    /**
     * Prints the separator at the end of the test
     *
     * @param testContext   Test context
     */
    @AfterEach
    default void afterEachTest(ExtensionContext testContext) {
        LOGGER.info(String.format("%s.%s-FINISHED", testContext.getRequiredTestClass().getName(), testContext.getRequiredTestMethod().getName()));
        LOGGER.info(String.join("", Collections.nCopies(76, SEPARATOR_CHAR)));
    }
}
