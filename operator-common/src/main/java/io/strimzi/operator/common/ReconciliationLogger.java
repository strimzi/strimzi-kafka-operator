/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.AbstractLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;
import org.apache.logging.log4j.util.MessageSupplier;
import org.apache.logging.log4j.util.Supplier;

import java.io.Serializable;

/**
 * Custom Logger interface with convenience methods for
 * the OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE and ALL custom log levels.
 * <p>Compatible with Log4j 2.6 or higher.</p>
 */
public class ReconciliationLogger implements Serializable {
    private static final long serialVersionUID = 258810740149174L;

    /**
     * Wrapped logger which we extend
     */
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

    protected ReconciliationLogger(final Logger logger) {
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
    
    ////// Operator Logging
    
    /**
     * Logs a message with the specific Marker at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     */
    public void offOp(final Marker marker, final Message msg) {
        logger.logIfEnabled(FQCN, OFF, marker, msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void offOp(final Marker marker, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, marker, msg, t);
    }

    /**
     * Logs a message object with the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void offOp(final Marker marker, final Object message) {
        logger.logIfEnabled(FQCN, OFF, marker, message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void offOp(final Marker marker, final CharSequence message) {
        logger.logIfEnabled(FQCN, OFF, marker, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code OFF} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void offOp(final Marker marker, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, marker, message, t);
    }

    /**
     * Logs a message at the {@code OFF} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void offOp(final Marker marker, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, marker, message, t);
    }

    /**
     * Logs a message object with the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void offOp(final Marker marker, final String message) {
        logger.logIfEnabled(FQCN, OFF, marker, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void offOp(final Marker marker, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, OFF, marker, message, params);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final Marker marker, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final Marker marker, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void offOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void offOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void offOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void offOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, OFF, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code OFF} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void offOp(final Marker marker, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, marker, message, t);
    }

    /**
     * Logs the specified Message at the {@code OFF} level.
     * 
     * @param msg the message string to be logged
     */
    public void offOp(final Message msg) {
        logger.logIfEnabled(FQCN, OFF, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code OFF} level.
     * 
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void offOp(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, null, msg, t);
    }

    /**
     * Logs a message object with the {@code OFF} level.
     * 
     * @param message the message object to log.
     */
    public void offOp(final Object message) {
        logger.logIfEnabled(FQCN, OFF, null, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code OFF} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void offOp(final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, null, message, t);
    }

    /**
     * Logs a message CharSequence with the {@code OFF} level.
     * 
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void offOp(final CharSequence message) {
        logger.logIfEnabled(FQCN, OFF, null, message, (Throwable) null);
    }

    /**
     * Logs a CharSequence at the {@code OFF} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void offOp(final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, null, message, t);
    }

    /**
     * Logs a message object with the {@code OFF} level.
     * 
     * @param message the message object to log.
     */
    public void offOp(final String message) {
        logger.logIfEnabled(FQCN, OFF, null, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void offOp(final String message, final Object... params) {
        logger.logIfEnabled(FQCN, OFF, null, message, params);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final String message, final Object p0) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void offOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
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
    public void offOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
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
    public void offOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
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
    public void offOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     * 
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
    public void offOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, OFF, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code OFF} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void offOp(final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, null, message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code OFF}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void offOp(final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, OFF, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code OFF}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void offOp(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code OFF} level with the specified Marker.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void offOp(final Marker marker, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, OFF, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code OFF} level.
     *
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void offOp(final Marker marker, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, OFF, marker, message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code OFF}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void offOp(final Marker marker, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, marker, msgSupplier, t);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is
     * the {@code OFF} level.
     *
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void offOp(final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, OFF, null, message, paramSuppliers);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code OFF} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void offOp(final Marker marker, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, OFF, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code OFF}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void offOp(final Marker marker, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, marker, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code OFF} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void offOp(final MessageSupplier msgSupplier) {
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
    public void offOp(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     */
    public void fatalOp(final Marker marker, final Message msg) {
        logger.logIfEnabled(FQCN, FATAL, marker, msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void fatalOp(final Marker marker, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, marker, msg, t);
    }

    /**
     * Logs a message object with the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void fatalOp(final Marker marker, final Object message) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void fatalOp(final Marker marker, final CharSequence message) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code FATAL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void fatalOp(final Marker marker, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, t);
    }

    /**
     * Logs a message at the {@code FATAL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void fatalOp(final Marker marker, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, t);
    }

    /**
     * Logs a message object with the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void fatalOp(final Marker marker, final String message) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void fatalOp(final Marker marker, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, params);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final Marker marker, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final Marker marker, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void fatalOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void fatalOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void fatalOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void fatalOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code FATAL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void fatalOp(final Marker marker, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, t);
    }

    /**
     * Logs the specified Message at the {@code FATAL} level.
     * 
     * @param msg the message string to be logged
     */
    public void fatalOp(final Message msg) {
        logger.logIfEnabled(FQCN, FATAL, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code FATAL} level.
     * 
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void fatalOp(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, null, msg, t);
    }

    /**
     * Logs a message object with the {@code FATAL} level.
     * 
     * @param message the message object to log.
     */
    public void fatalOp(final Object message) {
        logger.logIfEnabled(FQCN, FATAL, null, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code FATAL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void fatalOp(final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, null, message, t);
    }

    /**
     * Logs a message CharSequence with the {@code FATAL} level.
     * 
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void fatalOp(final CharSequence message) {
        logger.logIfEnabled(FQCN, FATAL, null, message, (Throwable) null);
    }

    /**
     * Logs a CharSequence at the {@code FATAL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void fatalOp(final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, null, message, t);
    }

    /**
     * Logs a message object with the {@code FATAL} level.
     * 
     * @param message the message object to log.
     */
    public void fatalOp(final String message) {
        logger.logIfEnabled(FQCN, FATAL, null, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void fatalOp(final String message, final Object... params) {
        logger.logIfEnabled(FQCN, FATAL, null, message, params);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final String message, final Object p0) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void fatalOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
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
    public void fatalOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
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
    public void fatalOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
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
    public void fatalOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     * 
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
    public void fatalOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, FATAL, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code FATAL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void fatalOp(final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, null, message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code FATAL}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void fatalOp(final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, FATAL, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code FATAL}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void fatalOp(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code FATAL} level with the specified Marker.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void fatalOp(final Marker marker, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, FATAL, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code FATAL} level.
     *
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void fatalOp(final Marker marker, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, FATAL, marker, message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code FATAL}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void fatalOp(final Marker marker, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, marker, msgSupplier, t);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is
     * the {@code FATAL} level.
     *
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void fatalOp(final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, FATAL, null, message, paramSuppliers);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code FATAL} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void fatalOp(final Marker marker, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, FATAL, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code FATAL}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void fatalOp(final Marker marker, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, marker, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code FATAL} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void fatalOp(final MessageSupplier msgSupplier) {
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
    public void fatalOp(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     */
    public void errorOp(final Marker marker, final Message msg) {
        logger.logIfEnabled(FQCN, ERROR, marker, msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void errorOp(final Marker marker, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, marker, msg, t);
    }

    /**
     * Logs a message object with the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void errorOp(final Marker marker, final Object message) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void errorOp(final Marker marker, final CharSequence message) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code ERROR} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void errorOp(final Marker marker, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, t);
    }

    /**
     * Logs a message at the {@code ERROR} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void errorOp(final Marker marker, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, t);
    }

    /**
     * Logs a message object with the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void errorOp(final Marker marker, final String message) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void errorOp(final Marker marker, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, params);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final Marker marker, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final Marker marker, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void errorOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void errorOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void errorOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void errorOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code ERROR} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void errorOp(final Marker marker, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, t);
    }

    /**
     * Logs the specified Message at the {@code ERROR} level.
     * 
     * @param msg the message string to be logged
     */
    public void errorOp(final Message msg) {
        logger.logIfEnabled(FQCN, ERROR, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code ERROR} level.
     * 
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void errorOp(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, null, msg, t);
    }

    /**
     * Logs a message object with the {@code ERROR} level.
     * 
     * @param message the message object to log.
     */
    public void errorOp(final Object message) {
        logger.logIfEnabled(FQCN, ERROR, null, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code ERROR} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void errorOp(final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, null, message, t);
    }

    /**
     * Logs a message CharSequence with the {@code ERROR} level.
     * 
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void errorOp(final CharSequence message) {
        logger.logIfEnabled(FQCN, ERROR, null, message, (Throwable) null);
    }

    /**
     * Logs a CharSequence at the {@code ERROR} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void errorOp(final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, null, message, t);
    }

    /**
     * Logs a message object with the {@code ERROR} level.
     * 
     * @param message the message object to log.
     */
    public void errorOp(final String message) {
        logger.logIfEnabled(FQCN, ERROR, null, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void errorOp(final String message, final Object... params) {
        logger.logIfEnabled(FQCN, ERROR, null, message, params);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final String message, final Object p0) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void errorOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
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
    public void errorOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
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
    public void errorOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
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
    public void errorOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     * 
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
    public void errorOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, ERROR, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code ERROR} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void errorOp(final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, null, message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code ERROR}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void errorOp(final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, ERROR, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ERROR}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void errorOp(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ERROR} level with the specified Marker.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void errorOp(final Marker marker, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, ERROR, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code ERROR} level.
     *
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void errorOp(final Marker marker, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, ERROR, marker, message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ERROR}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void errorOp(final Marker marker, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, marker, msgSupplier, t);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is
     * the {@code ERROR} level.
     *
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void errorOp(final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, ERROR, null, message, paramSuppliers);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ERROR} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void errorOp(final Marker marker, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, ERROR, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ERROR}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void errorOp(final Marker marker, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, marker, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ERROR} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void errorOp(final MessageSupplier msgSupplier) {
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
    public void errorOp(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     */
    public void warnOp(final Marker marker, final Message msg) {
        logger.logIfEnabled(FQCN, WARN, marker, msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void warnOp(final Marker marker, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, marker, msg, t);
    }

    /**
     * Logs a message object with the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void warnOp(final Marker marker, final Object message) {
        logger.logIfEnabled(FQCN, WARN, marker, message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void warnOp(final Marker marker, final CharSequence message) {
        logger.logIfEnabled(FQCN, WARN, marker, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code WARN} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void warnOp(final Marker marker, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, marker, message, t);
    }

    /**
     * Logs a message at the {@code WARN} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void warnOp(final Marker marker, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, marker, message, t);
    }

    /**
     * Logs a message object with the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void warnOp(final Marker marker, final String message) {
        logger.logIfEnabled(FQCN, WARN, marker, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void warnOp(final Marker marker, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, WARN, marker, message, params);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final Marker marker, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final Marker marker, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void warnOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void warnOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void warnOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void warnOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, WARN, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code WARN} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void warnOp(final Marker marker, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, marker, message, t);
    }

    /**
     * Logs the specified Message at the {@code WARN} level.
     * 
     * @param msg the message string to be logged
     */
    public void warnOp(final Message msg) {
        logger.logIfEnabled(FQCN, WARN, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code WARN} level.
     * 
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void warnOp(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, null, msg, t);
    }

    /**
     * Logs a message object with the {@code WARN} level.
     * 
     * @param message the message object to log.
     */
    public void warnOp(final Object message) {
        logger.logIfEnabled(FQCN, WARN, null, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code WARN} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void warnOp(final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, null, message, t);
    }

    /**
     * Logs a message CharSequence with the {@code WARN} level.
     * 
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void warnOp(final CharSequence message) {
        logger.logIfEnabled(FQCN, WARN, null, message, (Throwable) null);
    }

    /**
     * Logs a CharSequence at the {@code WARN} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void warnOp(final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, null, message, t);
    }

    /**
     * Logs a message object with the {@code WARN} level.
     * 
     * @param message the message object to log.
     */
    public void warnOp(final String message) {
        logger.logIfEnabled(FQCN, WARN, null, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void warnOp(final String message, final Object... params) {
        logger.logIfEnabled(FQCN, WARN, null, message, params);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final String message, final Object p0) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void warnOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
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
    public void warnOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
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
    public void warnOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
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
    public void warnOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     * 
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
    public void warnOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, WARN, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code WARN} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void warnOp(final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, null, message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code WARN}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void warnOp(final Supplier<?> msgSupplier) {
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
    public void warnOp(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code WARN} level with the specified Marker.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void warnOp(final Marker marker, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, WARN, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code WARN} level.
     *
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void warnOp(final Marker marker, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, WARN, marker, message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code WARN}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void warnOp(final Marker marker, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, marker, msgSupplier, t);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is
     * the {@code WARN} level.
     *
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void warnOp(final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, WARN, null, message, paramSuppliers);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code WARN} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void warnOp(final Marker marker, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, WARN, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code WARN}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void warnOp(final Marker marker, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, marker, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code WARN} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void warnOp(final MessageSupplier msgSupplier) {
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
    public void warnOp(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     */
    public void infoOp(final Marker marker, final Message msg) {
        logger.logIfEnabled(FQCN, INFO, marker, msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void infoOp(final Marker marker, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, marker, msg, t);
    }

    /**
     * Logs a message object with the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void infoOp(final Marker marker, final Object message) {
        logger.logIfEnabled(FQCN, INFO, marker, message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void infoOp(final Marker marker, final CharSequence message) {
        logger.logIfEnabled(FQCN, INFO, marker, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code INFO} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void infoOp(final Marker marker, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, marker, message, t);
    }

    /**
     * Logs a message at the {@code INFO} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void infoOp(final Marker marker, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, marker, message, t);
    }

    /**
     * Logs a message object with the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void infoOp(final Marker marker, final String message) {
        logger.logIfEnabled(FQCN, INFO, marker, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void infoOp(final Marker marker, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, INFO, marker, message, params);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final Marker marker, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final Marker marker, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void infoOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void infoOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void infoOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void infoOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, INFO, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code INFO} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void infoOp(final Marker marker, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, marker, message, t);
    }

    /**
     * Logs the specified Message at the {@code INFO} level.
     * 
     * @param msg the message string to be logged
     */
    public void infoOp(final Message msg) {
        logger.logIfEnabled(FQCN, INFO, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code INFO} level.
     * 
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void infoOp(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, null, msg, t);
    }

    /**
     * Logs a message object with the {@code INFO} level.
     * 
     * @param message the message object to log.
     */
    public void infoOp(final Object message) {
        logger.logIfEnabled(FQCN, INFO, null, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code INFO} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void infoOp(final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, null, message, t);
    }

    /**
     * Logs a message CharSequence with the {@code INFO} level.
     * 
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void infoOp(final CharSequence message) {
        logger.logIfEnabled(FQCN, INFO, null, message, (Throwable) null);
    }

    /**
     * Logs a CharSequence at the {@code INFO} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void infoOp(final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, null, message, t);
    }

    /**
     * Logs a message object with the {@code INFO} level.
     * 
     * @param message the message object to log.
     */
    public void infoOp(final String message) {
        logger.logIfEnabled(FQCN, INFO, null, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void infoOp(final String message, final Object... params) {
        logger.logIfEnabled(FQCN, INFO, null, message, params);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final String message, final Object p0) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void infoOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
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
    public void infoOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
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
    public void infoOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
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
    public void infoOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     * 
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
    public void infoOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, INFO, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code INFO} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void infoOp(final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, null, message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code INFO}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void infoOp(final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, INFO, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code INFO}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void infoOp(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code INFO} level with the specified Marker.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void infoOp(final Marker marker, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, INFO, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code INFO} level.
     *
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void infoOp(final Marker marker, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, INFO, marker, message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code INFO}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void infoOp(final Marker marker, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, marker, msgSupplier, t);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is
     * the {@code INFO} level.
     *
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void infoOp(final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, INFO, null, message, paramSuppliers);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code INFO} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void infoOp(final Marker marker, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, INFO, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code INFO}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void infoOp(final Marker marker, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, marker, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code INFO} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void infoOp(final MessageSupplier msgSupplier) {
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
    public void infoOp(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     */
    public void debugOp(final Marker marker, final Message msg) {
        logger.logIfEnabled(FQCN, DEBUG, marker, msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void debugOp(final Marker marker, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, marker, msg, t);
    }

    /**
     * Logs a message object with the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void debugOp(final Marker marker, final Object message) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void debugOp(final Marker marker, final CharSequence message) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code DEBUG} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void debugOp(final Marker marker, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, t);
    }

    /**
     * Logs a message at the {@code DEBUG} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void debugOp(final Marker marker, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, t);
    }

    /**
     * Logs a message object with the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void debugOp(final Marker marker, final String message) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void debugOp(final Marker marker, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, params);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final Marker marker, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final Marker marker, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void debugOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void debugOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void debugOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void debugOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code DEBUG} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void debugOp(final Marker marker, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, t);
    }

    /**
     * Logs the specified Message at the {@code DEBUG} level.
     * 
     * @param msg the message string to be logged
     */
    public void debugOp(final Message msg) {
        logger.logIfEnabled(FQCN, DEBUG, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code DEBUG} level.
     * 
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void debugOp(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, null, msg, t);
    }

    /**
     * Logs a message object with the {@code DEBUG} level.
     * 
     * @param message the message object to log.
     */
    public void debugOp(final Object message) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code DEBUG} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void debugOp(final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, t);
    }

    /**
     * Logs a message CharSequence with the {@code DEBUG} level.
     * 
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void debugOp(final CharSequence message) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, (Throwable) null);
    }

    /**
     * Logs a CharSequence at the {@code DEBUG} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void debugOp(final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, t);
    }

    /**
     * Logs a message object with the {@code DEBUG} level.
     * 
     * @param message the message object to log.
     */
    public void debugOp(final String message) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void debugOp(final String message, final Object... params) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, params);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final String message, final Object p0) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void debugOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
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
    public void debugOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
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
    public void debugOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
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
    public void debugOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     * 
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
    public void debugOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code DEBUG} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void debugOp(final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code DEBUG}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void debugOp(final Supplier<?> msgSupplier) {
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
    public void debugOp(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code DEBUG} level with the specified Marker.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void debugOp(final Marker marker, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, DEBUG, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code DEBUG} level.
     *
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void debugOp(final Marker marker, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, DEBUG, marker, message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code DEBUG}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void debugOp(final Marker marker, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, marker, msgSupplier, t);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is
     * the {@code DEBUG} level.
     *
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void debugOp(final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, DEBUG, null, message, paramSuppliers);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code DEBUG} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void debugOp(final Marker marker, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, DEBUG, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code DEBUG}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void debugOp(final Marker marker, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, marker, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code DEBUG} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void debugOp(final MessageSupplier msgSupplier) {
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
    public void debugOp(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     */
    public void traceOp(final Marker marker, final Message msg) {
        logger.logIfEnabled(FQCN, TRACE, marker, msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void traceOp(final Marker marker, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, marker, msg, t);
    }

    /**
     * Logs a message object with the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void traceOp(final Marker marker, final Object message) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void traceOp(final Marker marker, final CharSequence message) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code TRACE} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void traceOp(final Marker marker, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, t);
    }

    /**
     * Logs a message at the {@code TRACE} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void traceOp(final Marker marker, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, t);
    }

    /**
     * Logs a message object with the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void traceOp(final Marker marker, final String message) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void traceOp(final Marker marker, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, params);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final Marker marker, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final Marker marker, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void traceOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void traceOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void traceOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void traceOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code TRACE} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void traceOp(final Marker marker, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, t);
    }

    /**
     * Logs the specified Message at the {@code TRACE} level.
     * 
     * @param msg the message string to be logged
     */
    public void traceOp(final Message msg) {
        logger.logIfEnabled(FQCN, TRACE, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code TRACE} level.
     * 
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void traceOp(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, null, msg, t);
    }

    /**
     * Logs a message object with the {@code TRACE} level.
     * 
     * @param message the message object to log.
     */
    public void traceOp(final Object message) {
        logger.logIfEnabled(FQCN, TRACE, null, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code TRACE} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void traceOp(final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, null, message, t);
    }

    /**
     * Logs a message CharSequence with the {@code TRACE} level.
     * 
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void traceOp(final CharSequence message) {
        logger.logIfEnabled(FQCN, TRACE, null, message, (Throwable) null);
    }

    /**
     * Logs a CharSequence at the {@code TRACE} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void traceOp(final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, null, message, t);
    }

    /**
     * Logs a message object with the {@code TRACE} level.
     * 
     * @param message the message object to log.
     */
    public void traceOp(final String message) {
        logger.logIfEnabled(FQCN, TRACE, null, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void traceOp(final String message, final Object... params) {
        logger.logIfEnabled(FQCN, TRACE, null, message, params);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final String message, final Object p0) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void traceOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
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
    public void traceOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
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
    public void traceOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
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
    public void traceOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     * 
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
    public void traceOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, TRACE, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code TRACE} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void traceOp(final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, null, message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code TRACE}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void traceOp(final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, TRACE, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code TRACE}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void traceOp(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code TRACE} level with the specified Marker.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void traceOp(final Marker marker, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, TRACE, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code TRACE} level.
     *
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void traceOp(final Marker marker, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, TRACE, marker, message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code TRACE}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void traceOp(final Marker marker, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, marker, msgSupplier, t);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is
     * the {@code TRACE} level.
     *
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void traceOp(final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, TRACE, null, message, paramSuppliers);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code TRACE} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void traceOp(final Marker marker, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, TRACE, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code TRACE}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void traceOp(final Marker marker, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, marker, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code TRACE} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void traceOp(final MessageSupplier msgSupplier) {
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
    public void traceOp(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, null, msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     */
    public void allOp(final Marker marker, final Message msg) {
        logger.logIfEnabled(FQCN, ALL, marker, msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void allOp(final Marker marker, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, marker, msg, t);
    }

    /**
     * Logs a message object with the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void allOp(final Marker marker, final Object message) {
        logger.logIfEnabled(FQCN, ALL, marker, message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void allOp(final Marker marker, final CharSequence message) {
        logger.logIfEnabled(FQCN, ALL, marker, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code ALL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void allOp(final Marker marker, final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, marker, message, t);
    }

    /**
     * Logs a message at the {@code ALL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void allOp(final Marker marker, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, marker, message, t);
    }

    /**
     * Logs a message object with the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message object to log.
     */
    public void allOp(final Marker marker, final String message) {
        logger.logIfEnabled(FQCN, ALL, marker, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void allOp(final Marker marker, final String message, final Object... params) {
        logger.logIfEnabled(FQCN, ALL, marker, message, params);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final Marker marker, final String message, final Object p0) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final Marker marker, final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void allOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void allOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void allOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param marker the marker data specific to this log statement
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
    public void allOp(final Marker marker, final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, ALL, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code ALL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param marker the marker data specific to this log statement
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void allOp(final Marker marker, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, marker, message, t);
    }

    /**
     * Logs the specified Message at the {@code ALL} level.
     * 
     * @param msg the message string to be logged
     */
    public void allOp(final Message msg) {
        logger.logIfEnabled(FQCN, ALL, null, msg, (Throwable) null);
    }

    /**
     * Logs the specified Message at the {@code ALL} level.
     * 
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void allOp(final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, null, msg, t);
    }

    /**
     * Logs a message object with the {@code ALL} level.
     * 
     * @param message the message object to log.
     */
    public void allOp(final Object message) {
        logger.logIfEnabled(FQCN, ALL, null, message, (Throwable) null);
    }

    /**
     * Logs a message at the {@code ALL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void allOp(final Object message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, null, message, t);
    }

    /**
     * Logs a message CharSequence with the {@code ALL} level.
     * 
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void allOp(final CharSequence message) {
        logger.logIfEnabled(FQCN, ALL, null, message, (Throwable) null);
    }

    /**
     * Logs a CharSequence at the {@code ALL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the CharSequence to log.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.6
     */
    public void allOp(final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, null, message, t);
    }

    /**
     * Logs a message object with the {@code ALL} level.
     * 
     * @param message the message object to log.
     */
    public void allOp(final String message) {
        logger.logIfEnabled(FQCN, ALL, null, message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.

     */
    public void allOp(final String message, final Object... params) {
        logger.logIfEnabled(FQCN, ALL, null, message, params);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final String message, final Object p0) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final String message, final Object p0, final Object p1) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0, p1);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final String message, final Object p0, final Object p1, final Object p2) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0, p1, p2, p3, p4);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
     * @param message the message to log; the format depends on the message factory.
     * @param p0 parameter to the message.
     * @param p1 parameter to the message.
     * @param p2 parameter to the message.
     * @param p3 parameter to the message.
     * @param p4 parameter to the message.
     * @param p5 parameter to the message.

     * @since Log4j-2.6
     */
    public void allOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0, p1, p2, p3, p4, p5);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
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
    public void allOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0, p1, p2, p3, p4, p5, p6);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
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
    public void allOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0, p1, p2, p3, p4, p5, p6, p7);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
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
    public void allOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     * 
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
    public void allOp(final String message, final Object p0, final Object p1, final Object p2,
            final Object p3, final Object p4, final Object p5, final Object p6,
            final Object p7, final Object p8, final Object p9) {
        logger.logIfEnabled(FQCN, ALL, null, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
    }

    /**
     * Logs a message at the {@code ALL} level including the stack trace of
     * the {@link Throwable} {@code t} passed as parameter.
     * 
     * @param message the message to log.
     * @param t the exception to log, including its stack trace.
     */
    public void allOp(final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, null, message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code ALL}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void allOp(final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, ALL, null, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ALL}
     * level) including the stack trace of the {@link Throwable} <code>t</code> passed as parameter.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t the exception to log, including its stack trace.
     * @since Log4j-2.4
     */
    public void allOp(final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, null, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ALL} level with the specified Marker.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void allOp(final Marker marker, final Supplier<?> msgSupplier) {
        logger.logIfEnabled(FQCN, ALL, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is the
     * {@code ALL} level.
     *
     * @param marker the marker data specific to this log statement
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void allOp(final Marker marker, final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, ALL, marker, message, paramSuppliers);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ALL}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void allOp(final Marker marker, final Supplier<?> msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, marker, msgSupplier, t);
    }

    /**
     * Logs a message with parameters which are only to be constructed if the logging level is
     * the {@code ALL} level.
     *
     * @param message the message to log; the format depends on the message factory.
     * @param paramSuppliers An array of functions, which when called, produce the desired log message parameters.
     * @since Log4j-2.4
     */
    public void allOp(final String message, final Supplier<?>... paramSuppliers) {
        logger.logIfEnabled(FQCN, ALL, null, message, paramSuppliers);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ALL} level with the specified Marker. The {@code MessageSupplier} may or may
     * not use the {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void allOp(final Marker marker, final MessageSupplier msgSupplier) {
        logger.logIfEnabled(FQCN, ALL, marker, msgSupplier, (Throwable) null);
    }

    /**
     * Logs a message (only to be constructed if the logging level is the {@code ALL}
     * level) with the specified Marker and including the stack trace of the {@link Throwable}
     * <code>t</code> passed as parameter. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param marker the marker data specific to this log statement
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @param t A Throwable or null.
     * @since Log4j-2.4
     */
    public void allOp(final Marker marker, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, marker, msgSupplier, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the
     * {@code ALL} level. The {@code MessageSupplier} may or may not use the
     * {@link MessageFactory} to construct the {@code Message}.
     *
     * @param msgSupplier A function, which when called, produces the desired log message.
     * @since Log4j-2.4
     */
    public void allOp(final MessageSupplier msgSupplier) {
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
    public void allOp(final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, null, msgSupplier, t);
    }
    
    ////// CR logging

    /**
     * Logs a message with the specific Marker at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void offCr(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void offCr(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void offCr(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void offCr(final Reconciliation reconciliation, final CharSequence message) {
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
    public void offCr(final Reconciliation reconciliation, final Object message, final Throwable t) {
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
    public void offCr(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void offCr(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code OFF} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void offCr(final Reconciliation reconciliation, final String message, final Object... params) {
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0) {
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void offCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void offCr(final Reconciliation reconciliation, final String message, final Throwable t) {
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
    public void offCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
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
    public void offCr(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
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
    public void offCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
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
    public void offCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
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
    public void offCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, OFF, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void fatalCr(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void fatalCr(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void fatalCr(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void fatalCr(final Reconciliation reconciliation, final CharSequence message) {
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
    public void fatalCr(final Reconciliation reconciliation, final Object message, final Throwable t) {
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
    public void fatalCr(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void fatalCr(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code FATAL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object... params) {
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0) {
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
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
    public void fatalCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
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
    public void fatalCr(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
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
    public void fatalCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
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
    public void fatalCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
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
    public void fatalCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, FATAL, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void errorCr(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void errorCr(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void errorCr(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void errorCr(final Reconciliation reconciliation, final CharSequence message) {
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
    public void errorCr(final Reconciliation reconciliation, final Object message, final Throwable t) {
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
    public void errorCr(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void errorCr(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code ERROR} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void errorCr(final Reconciliation reconciliation, final String message, final Object... params) {
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0) {
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
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
    public void errorCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
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
    public void errorCr(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
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
    public void errorCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
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
    public void errorCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
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
    public void errorCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ERROR, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void warnCr(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void warnCr(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void warnCr(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void warnCr(final Reconciliation reconciliation, final CharSequence message) {
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
    public void warnCr(final Reconciliation reconciliation, final Object message, final Throwable t) {
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
    public void warnCr(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void warnCr(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code WARN} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void warnCr(final Reconciliation reconciliation, final String message, final Object... params) {
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0) {
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code WARN}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void warnCr(final Supplier<?> msgSupplier) {
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
    public void warnCr(final Supplier<?> msgSupplier, final Throwable t) {
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
    public void warnCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
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
    public void warnCr(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
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
    public void warnCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
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
    public void warnCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
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
    public void warnCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, WARN, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void infoCr(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void infoCr(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void infoCr(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void infoCr(final Reconciliation reconciliation, final CharSequence message) {
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
    public void infoCr(final Reconciliation reconciliation, final Object message, final Throwable t) {
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
    public void infoCr(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void infoCr(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code INFO} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void infoCr(final Reconciliation reconciliation, final String message, final Object... params) {
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0) {
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
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
    public void infoCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
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
    public void infoCr(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
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
    public void infoCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
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
    public void infoCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
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
    public void infoCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, INFO, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void debugCr(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void debugCr(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void debugCr(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void debugCr(final Reconciliation reconciliation, final CharSequence message) {
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
    public void debugCr(final Reconciliation reconciliation, final Object message, final Throwable t) {
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
    public void debugCr(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void debugCr(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code DEBUG} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void debugCr(final Reconciliation reconciliation, final String message, final Object... params) {
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0) {
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message which is only to be constructed if the logging level is the {@code DEBUG}level.
     *
     * @param msgSupplier A function, which when called, produces the desired log message;
     *            the format depends on the message factory.
     * @since Log4j-2.4
     */
    public void debugCr(final Supplier<?> msgSupplier) {
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
    public void debugCr(final Supplier<?> msgSupplier, final Throwable t) {
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
    public void debugCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
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
    public void debugCr(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
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
    public void debugCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
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
    public void debugCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
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
    public void debugCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, DEBUG, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void traceCr(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void traceCr(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void traceCr(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void traceCr(final Reconciliation reconciliation, final CharSequence message) {
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
    public void traceCr(final Reconciliation reconciliation, final Object message, final Throwable t) {
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
    public void traceCr(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void traceCr(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code TRACE} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void traceCr(final Reconciliation reconciliation, final String message, final Object... params) {
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0) {
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
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
    public void traceCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
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
    public void traceCr(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
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
    public void traceCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
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
    public void traceCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
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
    public void traceCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, TRACE, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * Logs a message with the specific Marker at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     */
    public void allCr(final Reconciliation reconciliation, final Message msg) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), msg, (Throwable) null);
    }

    /**
     * Logs a message with the specific Marker at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param msg the message string to be logged
     * @param t A Throwable or null.
     */
    public void allCr(final Reconciliation reconciliation, final Message msg, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), msg, t);
    }

    /**
     * Logs a message object with the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void allCr(final Reconciliation reconciliation, final Object message) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message CharSequence with the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message CharSequence to log.
     * @since Log4j-2.6
     */
    public void allCr(final Reconciliation reconciliation, final CharSequence message) {
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
    public void allCr(final Reconciliation reconciliation, final Object message, final Throwable t) {
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
    public void allCr(final Reconciliation reconciliation, final CharSequence message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
    }

    /**
     * Logs a message object with the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message object to log.
     */
    public void allCr(final Reconciliation reconciliation, final String message) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, (Throwable) null);
    }

    /**
     * Logs a message with parameters at the {@code ALL} level.
     *
     * @param reconciliation The reconciliation
     * @param message the message to log; the format depends on the message factory.
     * @param params parameters to the message.
     */
    public void allCr(final Reconciliation reconciliation, final String message, final Object... params) {
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0) {
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1) {
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2) {
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void allCr(final Reconciliation reconciliation, final String message, final Object p0, final Object p1, final Object p2,
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
    public void allCr(final Reconciliation reconciliation, final String message, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), reconciliation.toString() + ": " + message, t);
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
    public void allCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier) {
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
    public void allCr(final Reconciliation reconciliation, final String message, final Supplier<?>... paramSuppliers) {
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
    public void allCr(final Reconciliation reconciliation, final Supplier<?> msgSupplier, final Throwable t) {
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
    public void allCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier) {
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
    public void allCr(final Reconciliation reconciliation, final MessageSupplier msgSupplier, final Throwable t) {
        logger.logIfEnabled(FQCN, ALL, reconciliation.getMarker(), msgSupplier, t);
    }

    /**
     * @return  True if fatal logging is enabled. False otherwise.
     */
    public boolean isFatalEnabled() {
        return logger.isFatalEnabled();
    }

    /**
     * @return  True if error logging is enabled. False otherwise.
     */
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    /**
     * @return  True if warning logging is enabled. False otherwise.
     */
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    /**
     * @return  True if info logging is enabled. False otherwise.
     */
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    /**
     * @return  True if debug logging is enabled. False otherwise.
     */
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    /**
     * @return  True if trace logging is enabled. False otherwise.
     */
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }
}

