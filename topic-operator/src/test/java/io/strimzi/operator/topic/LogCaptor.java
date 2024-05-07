/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.LevelRangeFilter;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;

import java.lang.reflect.Field;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * A mechanism for asserting that messages are logged, or not logged, through Log4j2.
 * <pre>
 * {@code
 *         try (var captor = LogCaptor.requireMatch(LOGGER, Level.INFO,
 *                 le -> "Hello, world".equals(le.getMessage().getFormattedMessage()),
 *                 50, TimeUnit.MILLISECONDS)) {
 *             LOGGER.info("Hello, world");
 *         }
 * }
 * </pre>
 */
public class LogCaptor implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(LogCaptor.class);

    private final CaptorAppender appender;
    private final AbstractLogger logger;
    private final Level originalLevel;
    private final CountDownLatch latch;
    private final long timeout;
    private final TimeUnit unit;
    private final String timeoutMessage;

    private LogCaptor(AbstractLogger logger,
                      Level originalLevel,
                      CaptorAppender captorAppender,
                      CountDownLatch latch,
                      long timeout,
                      TimeUnit unit,
                      String timeoutMessage) {
        this.logger = logger;
        this.originalLevel = originalLevel;
        this.appender = captorAppender;
        this.latch = latch;
        this.timeout = timeout;
        this.unit = unit;
        this.timeoutMessage = timeoutMessage;
    }

    /**
     * Create a captor
     * @param logger The logger though which the log message should (or should not) be omitted.
     * @param level The level of the required message
     * @param predicate A predicate (e.g. on the message text)
     * @param timeout A timeout
     * @param unit A timeout unit
     * @return
     */
    public static LogCaptor logEventMatches(Logger logger,
                                            Level level,
                                            Predicate<LogEvent> predicate,
                                            long timeout,
                                            TimeUnit unit) {
        AbstractLogger theLogger = (AbstractLogger) logger;
        LevelRangeFilter filter = LevelRangeFilter.createFilter(level, level, Filter.Result.ACCEPT, Filter.Result.DENY);
        var latch = new CountDownLatch(1);
        CaptorAppender captorAppender = new CaptorAppender(
                "CAPTOR",
                filter,
                predicate,
                logEvent -> latch.countDown());

        captorAppender.start();

        Level originalLevel = setLevel(theLogger, level);
        if (theLogger instanceof org.apache.logging.log4j.core.Logger) {
            LOGGER.debug("Installed CaptorAppender on {} at {}", theLogger.getName(), level);
            ((org.apache.logging.log4j.core.Logger) theLogger).addAppender(captorAppender);
        }
        return new LogCaptor(theLogger, originalLevel, captorAppender, latch, timeout, unit,
                "No events logged to " + logger.getName() + " at level " + level + " and " + predicate + " observed within " + timeout + " " + unit.toString().toLowerCase(Locale.ENGLISH));
    }

    private static Level setLevel(AbstractLogger theLogger, Level level) {
        Level originalLevel = theLogger.getLevel();
        if (theLogger instanceof org.apache.logging.log4j.core.Logger) {
            LoggerContext context = ((org.apache.logging.log4j.core.Logger) theLogger).getContext();
            context.getConfiguration().getLoggerConfig(theLogger.getName()).setLevel(level);
            context.updateLoggers();
        }
        return originalLevel;
    }

    /**
     * Create a captor
     * @param logger The logger though which the log message should (or should not) be omitted.
     * @param level The level of the required message
     * @param predicate A predicate (e.g. on the message text)
     * @param timeout A timeout
     * @param unit A timeout unit
     * @return
     */
    public static LogCaptor logEventMatches(ReconciliationLogger logger,
                              Level level,
                              Predicate<LogEvent> predicate,
                              long timeout,
                              TimeUnit unit) {
        try {
            Field field1 = ReconciliationLogger.class.getDeclaredField("logger");
            field1.setAccessible(true);
            ExtendedLoggerWrapper loggerWrapper = (ExtendedLoggerWrapper) field1.get(logger);
            Field field2 = ExtendedLoggerWrapper.class.getDeclaredField("logger");
            field2.setAccessible(true);
            ExtendedLogger extendedLogger = (ExtendedLogger) field2.get(loggerWrapper);
            return logEventMatches(extendedLogger, level, predicate, timeout, unit);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a captor
     * @param logger The logger though which the log message should (or should not) be omitted.
     * @param level The level of the required message
     * @param messageRegex A regex that the log message much match
     * @param timeout A timeout
     * @param unit A timeout unit
     * @return
     */
    public static LogCaptor logMessageMatches(ReconciliationLogger logger,
                                              Level level,
                                              String messageRegex,
                                              long timeout,
                                              TimeUnit unit) {

        Predicate<LogEvent> logEventPredicate = messageContainsMatch(messageRegex);
        return logEventMatches(logger, level, logEventPredicate, timeout, unit);
    }

    public static Predicate<LogEvent> messageContainsMatch(String messageRegex) {
        return new Predicate<>() {
            private final Pattern pattern = Pattern.compile(messageRegex);
            @Override
            public boolean test(LogEvent le) {
                return pattern.matcher(le.getMessage().getFormattedMessage()).find();
            }

            @Override
            public String toString() {
                return "with a message containing a substring matching '" + messageRegex + "'";
            }
        };
    }

    /**
     * Create a captor
     * @param logger The logger though which the log message should (or should not) be omitted.
     * @param level The level of the required message
     * @param format The required message template text
     * @param timeout A timeout
     * @param unit A timeout unit
     * @return
     */
    public static LogCaptor logFormatEquals(Logger logger,
                                            Level level,
                                            String format,
                                            long timeout,
                                            TimeUnit unit) {
        return logEventMatches(logger, level, le -> format.equals(le.getMessage().getFormat()), timeout, unit);
    }

    /**
     * Cleans up the logging changes required by the captor
     * and either returns when the first matching log event is observed,
     * or throws a TimeoutException if the pre-configured timeout was reached before
     * any matching log event was observed.
     */
    @Override
    public void close() throws TimeoutException, InterruptedException {
        boolean timeout = !latch.await(this.timeout, unit);
        if (appender.isStarted()) {
            if (logger instanceof org.apache.logging.log4j.core.Logger) {
                ((org.apache.logging.log4j.core.Logger) logger).removeAppender(appender);
            }
            setLevel(logger, originalLevel);
            appender.stop();
        }
        if (timeout) {
            throw new TimeoutException(timeoutMessage);
        }
    }

    static class CaptorAppender extends AbstractAppender {

        private final Predicate<LogEvent> predicate;
        private final Consumer<LogEvent> consumer;

        public CaptorAppender(final String name,
                              final Filter filter,
                              Predicate<LogEvent> predicate,
                              Consumer<LogEvent> consumer) {
            super(name, filter, null, false, null);
            this.predicate = predicate;
            this.consumer = consumer;
        }

        @Override
        public void append(LogEvent event) {
            if (predicate.test(event)) {
                consumer.accept(event);
            }
        }
    }
}
