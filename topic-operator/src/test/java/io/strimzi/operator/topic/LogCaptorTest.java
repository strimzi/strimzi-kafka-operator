/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertThrows;

class LogCaptorTest {

    private static final Logger LOGGER = LogManager.getLogger(LogCaptorTest.class);

    @Test
    void shouldCaptureInfo() throws InterruptedException, TimeoutException {
        try (var captor = LogCaptor.logEventMatches(LOGGER, Level.INFO,
                le -> "Hello, world".equals(le.getMessage().getFormattedMessage()),
                50, TimeUnit.MILLISECONDS)) {
            LOGGER.info("Hello, world");
        }
    }

    @Test
    void shouldTimeoutIfLevelDoesNotMatch() throws InterruptedException, TimeoutException {
        assertThrows(TimeoutException.class, () -> {
            try (var captor = LogCaptor.logEventMatches(LOGGER, Level.INFO,
                    le -> "Hello, world".equals(le.getMessage().getFormattedMessage()),
                    50, TimeUnit.MILLISECONDS)) {
                LOGGER.debug("Hello, world");
            }
        });
    }

    @Test
    void shouldTimeoutIfPrediciateNotSatisfied() throws InterruptedException, TimeoutException {
        assertThrows(TimeoutException.class, () -> {
            try (var captor = LogCaptor.logEventMatches(LOGGER, Level.INFO,
                    le -> "Hello, world".equals(le.getMessage().getFormattedMessage()),
                    50, TimeUnit.MILLISECONDS)) {
                LOGGER.info("Hello, world 3");
            }
        });

    }
}
