/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import java.io.Serializable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;
import org.apache.logging.log4j.util.MessageSupplier;
import org.apache.logging.log4j.util.Supplier;

/**
 * Custom Logger interface with convenience methods for
 * the OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE and ALL custom log levels.
 * <p>Compatible with Log4j 2.6 or higher.</p>
 */
public final class ReconciliationLogger implements Serializable {
    private static final long serialVersionUID = 258810740149174L;
    private final ExtendedLoggerWrapper logger;

    private static final String FQCN = ReconciliationLogger.class.getName();
    private static final Level OFF = Level.forName("OFF", 0);
    private static final Level FATAL = Level.forName("FATAL", 100);
    private static final Level ERROR = Level.forName("ERROR", 200);
    private static final Level WARN = Level.forName("WARN", 300);
    private static final Level INFO = Level.forName("INFO", 400);
    private static final Level DEBUG = Level.forName("DEBUG", 500);
    private static final Level TRACE = Level.forName("TRACE", 600);
    private static final Level ALL = Level.forName("ALL", 10000);

    private ReconciliationLogger(final Logger logger) {
        this.logger = new ExtendedLoggerWrapper((AbstractLogger) logger, logger.getName(), logger.getMessageFactory());
    }

    /**
     * Returns a custom Logger with the name of the calling class.
     * 
     * @return The custom Logger for the calling class.
     */
    public static ReconciliationLogger create() {
        final Logger wrapped = LogManager.getLogger();
        return new ReconciliationLogger(wrapped);
    }

    /**
     * Returns a custom Logger using the fully qualified name of the Class as
     * the Logger name.
     * 
     * @param loggerName The Class whose name should be used as the Logger name.
     *            If null it will default to the calling class.
     * @return The custom Logger.
     */
    public static ReconciliationLogger create(final Class<?> loggerName) {
        final Logger wrapped = LogManager.getLogger(loggerName);
        return new ReconciliationLogger(wrapped);
    }

    /**
     * Returns a custom Logger using the fully qualified name of the Class as
     * the Logger name.
     * 
     * @param loggerName The Class whose name should be used as the Logger name.
     *            If null it will default to the calling class.
     * @param messageFactory The message factory is used only when creating a
     *            logger, subsequent use does not change the logger but will log
     *            a warning if mismatched.
     * @return The custom Logger.
     */
    public static ReconciliationLogger create(final Class<?> loggerName, final MessageFactory messageFactory) {
        final Logger wrapped = LogManager.getLogger(loggerName, messageFactory);
        return new ReconciliationLogger(wrapped);
    }

    /**
     * Returns a custom Logger using the fully qualified class name of the value
     * as the Logger name.
     * 
     * @param value The value whose class name should be used as the Logger
     *            name. If null the name of the calling class will be used as
     *            the logger name.
     * @return The custom Logger.
     */
    public static ReconciliationLogger create(final Object value) {
        final Logger wrapped = LogManager.getLogger(value);
        return new ReconciliationLogger(wrapped);
    }

    /**
     * Returns a custom Logger using the fully qualified class name of the value
     * as the Logger name.
     * 
     * @param value The value whose class name should be used as the Logger
     *            name. If null the name of the calling class will be used as
     *            the logger name.
     * @param messageFactory The message factory is used only when creating a
     *            logger, subsequent use does not change the logger but will log
     *            a warning if mismatched.
     * @return The custom Logger.
     */
    public static ReconciliationLogger create(final Object value, final MessageFactory messageFactory) {
        final Logger wrapped = LogManager.getLogger(value, messageFactory);
        return new ReconciliationLogger(wrapped);
    }

    /**
     * Returns a custom Logger with the specified name.
     * 
     * @param name The logger name. If null the name of the calling class will
     *            be used.
     * @return The custom Logger.
     */
    public static ReconciliationLogger create(final String name) {
        final Logger wrapped = LogManager.getLogger(name);
        return new ReconciliationLogger(wrapped);
    }

    /**
     * Returns a custom Logger with the specified name.
     * 
     * @param name The logger name. If null the name of the calling class will
     *            be used.
     * @param messageFactory The message factory is used only when creating a
     *            logger, subsequent use does not change the logger but will log
     *            a warning if mismatched.
     * @return The custom Logger.
     */
    public static ReconciliationLogger create(final String name, final MessageFactory messageFactory) {
        final Logger wrapped = LogManager.getLogger(name, messageFactory);
        return new ReconciliationLogger(wrapped);
    }

    /**
     * Logs a message with the specific Marker at the {@code OFF} level.
     * 
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void off(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code OFF} level.
     * 
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void off(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code OFF} level.
     * 
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void off(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code OFF} level.
     * 
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final CharSequence message) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code OFF} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void off(final Reconciliation reconciliation, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message at the {@code OFF} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param reconciliation The reconciliation
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code OFF} level.
     * 
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void off(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void off(final Reconciliation reconciliation, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, params);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @param p9 parameter to the message.
     * @since Log4j-2.6
     */
    public void off(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code OFF} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void off(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code OFF} level with the specified Marker.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void off(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void off(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code OFF}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void off(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code OFF} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void off(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code OFF}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void off(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code OFF} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void off(final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, OFF, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code OFF}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     * The {@code MessageSupplier} may or may not use the {@link MessageFactory} to construct the
     * {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void off(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void fatal(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void fatal(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void fatal(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final CharSequence message) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code FATAL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void fatal(final Reconciliation reconciliation, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message at the {@code FATAL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void fatal(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, params);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @param p9 parameter to the message.
     * @since Log4j-2.6
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code FATAL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs the specified Message at the {@code FATAL} level.
     *
     * @param msg the message string to be logged
     */
    public void fatal(final Message msg) {
        logger.logIfEnabled(FQCN, FATAL, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code FATAL} level.
     *
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void fatal(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, null, msg, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code FATAL} level with the specified Marker.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void fatal(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void fatal(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code FATAL}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void fatal(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code FATAL} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void fatal(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code FATAL}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void fatal(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code FATAL} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void fatal(final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, FATAL, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code FATAL}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     * The {@code MessageSupplier} may or may not use the {@link MessageFactory} to construct the
     * {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void fatal(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void error(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void error(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void error(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final CharSequence message) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code ERROR} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void error(final Reconciliation reconciliation, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message at the {@code ERROR} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void error(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void error(final Reconciliation reconciliation, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, params);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @param p9 parameter to the message.
     * @since Log4j-2.6
     */
    public void error(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code ERROR} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void error(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs the specified Message at the {@code ERROR} level.
     *
     * @param msg the message string to be logged
     */
    public void error(final Message msg) {
        logger.logIfEnabled(FQCN, ERROR, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code ERROR} level.
     *
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void error(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, null, msg, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ERROR} level with the specified Marker.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void error(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void error(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ERROR}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void error(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ERROR} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void error(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ERROR}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void error(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ERROR} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void error(final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, ERROR, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ERROR}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     * The {@code MessageSupplier} may or may not use the {@link MessageFactory} to construct the
     * {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void error(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void warn(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void warn(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void warn(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final CharSequence message) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code WARN} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void warn(final Reconciliation reconciliation, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message at the {@code WARN} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void warn(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, params);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @param p9 parameter to the message.
     * @since Log4j-2.6
     */
    public void warn(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code WARN} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void warn(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs the specified Message at the {@code WARN} level.
     *
     * @param msg the message string to be logged
     */
    public void warn(final Message msg) {
        logger.logIfEnabled(FQCN, WARN, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code WARN} level.
     *
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void warn(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, null, msg, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code WARN}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void warn(final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, WARN, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code WARN}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void warn(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code WARN} level with the specified Marker.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void warn(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void warn(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code WARN}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void warn(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code WARN} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void warn(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code WARN}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void warn(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code WARN} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void warn(final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, WARN, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code WARN}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     * The {@code MessageSupplier} may or may not use the {@link MessageFactory} to construct the
     * {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void warn(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void info(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void info(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void info(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final CharSequence message) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code INFO} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void info(final Reconciliation reconciliation, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message at the {@code INFO} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void info(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void info(final Reconciliation reconciliation, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, params);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @param p9 parameter to the message.
     * @since Log4j-2.6
     */
    public void info(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code INFO} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void info(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs the specified Message at the {@code INFO} level.
     *
     * @param msg the message string to be logged
     */
    public void info(final Message msg) {
        logger.logIfEnabled(FQCN, INFO, null, msg, (Throwable) null);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code INFO} level with the specified Marker.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void info(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void info(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code INFO}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void info(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code INFO} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void info(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code INFO}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void info(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code INFO} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void info(final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, INFO, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code INFO}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     * The {@code MessageSupplier} may or may not use the {@link MessageFactory} to construct the
     * {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void info(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void debug(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void debug(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void debug(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final CharSequence message) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code DEBUG} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void debug(final Reconciliation reconciliation, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message at the {@code DEBUG} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void debug(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, params);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @param p9 parameter to the message.
     * @since Log4j-2.6
     */
    public void debug(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code DEBUG} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void debug(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs the specified Message at the {@code DEBUG} level.
     *
     * @param msg the message string to be logged
     */
    public void debug(final Message msg) {
        logger.logIfEnabled(FQCN, DEBUG, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code DEBUG} level.
     *
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void debug(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, null, msg, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code DEBUG}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void debug(final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, DEBUG, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code DEBUG}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void debug(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code DEBUG} level with the specified Marker.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void debug(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void debug(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code DEBUG}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void debug(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code DEBUG} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void debug(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code DEBUG}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void debug(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code DEBUG} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void debug(final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, DEBUG, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code DEBUG}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     * The {@code MessageSupplier} may or may not use the {@link MessageFactory} to construct the
     * {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void debug(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void trace(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void trace(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void trace(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final CharSequence message) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code TRACE} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void trace(final Reconciliation reconciliation, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message at the {@code TRACE} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void trace(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, params);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @param p9 parameter to the message.
     * @since Log4j-2.6
     */
    public void trace(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code TRACE} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void trace(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs the specified Message at the {@code TRACE} level.
     *
     * @param msg the message string to be logged
     */
    public void trace(final Message msg) {
        logger.logIfEnabled(FQCN, TRACE, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code TRACE} level.
     *
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void trace(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, null, msg, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code TRACE} level with the specified Marker.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void trace(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void trace(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code TRACE}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void trace(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code TRACE} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void trace(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code TRACE}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void trace(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code TRACE} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void trace(final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, TRACE, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code TRACE}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     * The {@code MessageSupplier} may or may not use the {@link MessageFactory} to construct the
     * {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void trace(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void all(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void all(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void all(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final CharSequence message) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code ALL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void all(final Reconciliation reconciliation, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message at the {@code ALL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void all(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void all(final Reconciliation reconciliation, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, params);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.
     * @param p6 parameter to the message.
     * @param p7 parameter to the message.
     * @param p8 parameter to the message.
     * @param p9 parameter to the message.
     * @since Log4j-2.6
     */
    public void all(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code ALL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void all(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs the specified Message at the {@code ALL} level.
     *
     * @param msg the message string to be logged
     */
    public void all(final Message msg) {
        logger.logIfEnabled(FQCN, ALL, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code ALL} level.
     *
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void all(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, null, msg, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ALL} level with the specified Marker.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void all(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void all(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ALL}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void all(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ALL} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void all(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ALL}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param reconciliation The reconciliation
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void all(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ALL} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void all(final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, ALL, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ALL}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     * The {@code MessageSupplier} may or may not use the {@link MessageFactory} to construct the
     * {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void all(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, null, msgSupplier, t);
    }
}

