/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.message.FormattedMessageFactory;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class TestLogger extends AbstractLogger {
    public static class LoggedMessage {
        private final Level level;
        private final Marker marker;
        private final String formattedMessage;
        private final Throwable throwable;

        LoggedMessage(Level level, Marker marker, Message message, Throwable throwable) {
            this.level = level;
            this.marker = marker;
            this.formattedMessage = message.getFormattedMessage();
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

    private final Logger delegate;
    private List<LoggedMessage> loggedMessages = new ArrayList<>();

    public TestLogger(Logger delegate) {
        super("test", new FormattedMessageFactory());
        this.delegate = delegate;
    }

    public List<LoggedMessage> getLoggedMessages() {
        return loggedMessages;
    }

    public void assertLoggedAtLeastOnce(Predicate<LoggedMessage> test) {
        Assert.assertTrue("Expected message was not logged", getLoggedMessages().stream().anyMatch(test));
    }

    @Override
    public Level getLevel() {
        return Level.TRACE;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, Message message, Throwable throwable) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, CharSequence charSequence, Throwable throwable) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, Object o, Throwable throwable) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Throwable throwable) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object... objects) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o, Object o1) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o, Object o1, Object o2) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8) {
        return true;
    }

    @Override
    public boolean isEnabled(Level level, Marker marker, String s, Object o, Object o1, Object o2, Object o3, Object o4, Object o5, Object o6, Object o7, Object o8, Object o9) {
        return true;
    }

    @Override
    public void logMessage(String s, Level level, Marker marker, Message message, Throwable throwable) {
        if (delegate != null) {
            delegate.logMessage(s, level, marker, message, throwable);
        }
        loggedMessages.add(new LoggedMessage(level, marker, message, throwable));
    }
}
