/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.topic;

import org.apache.logging.log4j.Logger;


/**
 * This class wraps up the Log4j2 loggers. It adds the markers that can be used for log filtering.
 * Because TO uses logContext (not the Reconciliation) this class has to be duplicate of io.strimzi.operator.common.ReconciliationLogger.
 */
public class ReconciliationLogger {
    private final Logger log4j2Logger;
    public ReconciliationLogger(Logger log4j2Logger) {
        this.log4j2Logger = log4j2Logger;
    }
    // INFO
    public void info(LogContext ctx, String msg) {
        this.log4j2Logger.info(ctx.getMarker(), "{}: " + msg, ctx);
    }

    public void info(LogContext ctx, String msg, Object arg) {
        this.log4j2Logger.info(ctx.getMarker(), "{}: " + msg, ctx, arg);
    }

    public void info(LogContext ctx, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.info(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2);
    }

    public void info(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.info(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3);
    }

    public void info(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3, Object arg4) {
        this.log4j2Logger.info(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3, arg4);
    }

    public void info(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        this.log4j2Logger.info(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3, arg4, arg5);
    }

    public void info(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        this.log4j2Logger.info(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    // DEBUG
    public void debug(LogContext ctx, String msg) {
        this.log4j2Logger.debug(ctx.getMarker(), "{}: " + msg, ctx);
    }

    public void debug(LogContext ctx, String msg, Object arg) {
        this.log4j2Logger.debug(ctx.getMarker(), "{}: " + msg, ctx, arg);
    }

    public void debug(LogContext ctx, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.debug(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2);
    }

    public void debug(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.debug(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3);
    }

    public void debug(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3, Object arg4) {
        this.log4j2Logger.debug(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3, arg4);
    }

    public void debug(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        this.log4j2Logger.debug(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3, arg4, arg5);
    }

    public void debug(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        this.log4j2Logger.debug(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    public void debug(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7) {
        this.log4j2Logger.debug(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    }

    // WARN
    public void warn(LogContext ctx, String msg) {
        this.log4j2Logger.warn(ctx.getMarker(), "{}: " + msg, ctx);
    }

    public void warn(LogContext ctx, String msg, Object arg) {
        this.log4j2Logger.warn(ctx.getMarker(), "{}: " + msg, ctx, arg);
    }

    public void warn(LogContext ctx, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.warn(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2);
    }

    public void warn(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.warn(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3);
    }

    // FATAL
    public void fatal(LogContext ctx, String msg) {
        this.log4j2Logger.fatal(ctx.getMarker(), "{}: " + msg, ctx);
    }

    public void fatal(LogContext ctx, String msg, Object arg) {
        this.log4j2Logger.fatal(ctx.getMarker(), "{}: " + msg, ctx, arg);
    }

    public void fatal(LogContext ctx, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.fatal(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2);
    }

    public void fatal(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.fatal(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3);
    }

    // ERROR
    public void error(LogContext ctx, String msg) {
        this.log4j2Logger.error(ctx.getMarker(), "{}: " + msg, ctx);
    }

    public void error(LogContext ctx, String msg, Object arg) {
        this.log4j2Logger.error(ctx.getMarker(), "{}: " + msg, ctx, arg);
    }

    public void error(LogContext ctx, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.error(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2);
    }

    public void error(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.error(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3);
    }

    // TRACE
    public void trace(LogContext ctx, String msg) {
        this.log4j2Logger.trace(ctx.getMarker(), "{}: " + msg, ctx);
    }

    public void trace(LogContext ctx, String msg, Object arg) {
        this.log4j2Logger.trace(ctx.getMarker(), "{}: " + msg, ctx, arg);
    }

    public void trace(LogContext ctx, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.trace(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2);
    }

    public void trace(LogContext ctx, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.trace(ctx.getMarker(), "{}: " + msg, ctx, arg1, arg2, arg3);
    }

}
