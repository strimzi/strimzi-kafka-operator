/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.logging;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestLogger extends ReconciliationLogger {
    public static class LoggedMessage {
        private final Level level;
        private final Marker marker;
        private final String formattedMessage;
        private final Throwable throwable;

        public LoggedMessage(Level level, Marker marker, String msg, Throwable throwable) {
            this.level = level;
            this.marker = marker;
            this.formattedMessage = msg;
            this.throwable = throwable;
        }

        public Level level() {
            return level;
        }

        public Marker marker() {
            return marker;
        }

        public String formattedMessage() {
            return formattedMessage;
        }

        public Throwable throwable() {
            return throwable;
        }
    }

    private TestLogger(final Logger logger) {
        super(new ExtendedLoggerWrapper((AbstractLogger) logger, logger.getName(), logger.getMessageFactory()));
    }

    public static TestLogger create(final Class<?> loggerName) {
        final Logger wrapped = LogManager.getLogger(loggerName);
        return new TestLogger(wrapped);
    }

    private List<LoggedMessage> loggedMessages = new ArrayList<>();

    public List<LoggedMessage> getLoggedMessages() {
        return loggedMessages;
    }

    public void assertLoggedAtLeastOnce(Predicate<LoggedMessage> test) {
        assertThat("Expected message was not logged", getLoggedMessages().stream().anyMatch(test), is(true));
    }

    public void assertNotLogged(Predicate<LoggedMessage> test) {
        assertThat("Unexpected message was logged", getLoggedMessages().stream().noneMatch(test), is(true));
    }

    @Override
    public void warnCr(Reconciliation reconciliation, String msg) {
        loggedMessages.add(new LoggedMessage(Level.WARN, null, reconciliation.toString() + ": " + msg, null));
    }
}
