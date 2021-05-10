/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.operator.common;

import org.apache.logging.log4j.Logger;

/**
 * This class wraps up the Log4j2 loggers. It adds the markers that can be used for log filtering.
 */
public class ReconciliationLogger {
    private final Logger log4j2Logger;
    public ReconciliationLogger(Logger log4j2Logger) {
        this.log4j2Logger = log4j2Logger;
    }
    // INFO
    public void info(Reconciliation r, String msg) {
        this.log4j2Logger.info(r.getMarker(), "{}: " + msg, r);
    }

    public void info(Reconciliation r, String msg, Object arg) {
        this.log4j2Logger.info(r.getMarker(), "{}: " + msg, r, arg);
    }

    public void info(Reconciliation r, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.info(r.getMarker(), "{}: " + msg, r, arg1, arg2);
    }

    public void info(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.info(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3);
    }

    public void info(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3, Object arg4) {
        this.log4j2Logger.info(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3, arg4);
    }

    public void info(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        this.log4j2Logger.info(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3, arg4, arg5);
    }

    public void info(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        this.log4j2Logger.info(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    // DEBUG
    public void debug(Reconciliation r, String msg) {
        this.log4j2Logger.debug(r.getMarker(), "{}: " + msg, r);
    }

    public void debug(Reconciliation r, String msg, Object arg) {
        this.log4j2Logger.debug(r.getMarker(), "{}: " + msg, r, arg);
    }

    public void debug(Reconciliation r, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.debug(r.getMarker(), "{}: " + msg, r, arg1, arg2);
    }

    public void debug(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.debug(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3);
    }

    public void debug(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3, Object arg4) {
        this.log4j2Logger.debug(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3, arg4);
    }

    public void debug(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        this.log4j2Logger.debug(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3, arg4, arg5);
    }

    public void debug(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        this.log4j2Logger.debug(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    public void debug(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6, Object arg7) {
        this.log4j2Logger.debug(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    }

    // WARN
    public void warn(Reconciliation r, String msg) {
        this.log4j2Logger.warn(r.getMarker(), "{}: " + msg, r);
    }

    public void warn(Reconciliation r, String msg, Object arg) {
        this.log4j2Logger.warn(r.getMarker(), "{}: " + msg, r, arg);
    }

    public void warn(Reconciliation r, Object arg) {
        this.log4j2Logger.warn(r.getMarker(), "{}: {}", r, arg);
    }

    public void warn(Reconciliation r, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.warn(r.getMarker(), "{}: " + msg, r, arg1, arg2);
    }

    public void warn(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.warn(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3);
    }

    // FATAL
    public void fatal(Reconciliation r, String msg) {
        this.log4j2Logger.fatal(r.getMarker(), "{}: " + msg, r);
    }

    public void fatal(Reconciliation r, String msg, Object arg) {
        this.log4j2Logger.fatal(r.getMarker(), "{}: " + msg, r, arg);
    }

    public void fatal(Reconciliation r, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.fatal(r.getMarker(), "{}: " + msg, r, arg1, arg2);
    }

    public void fatal(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.fatal(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3);
    }

    // ERROR
    public void error(Reconciliation r, String msg) {
        this.log4j2Logger.error(r.getMarker(), "{}: " + msg, r);
    }

    public void error(Reconciliation r, String msg, Object arg) {
        this.log4j2Logger.error(r.getMarker(), "{}: " + msg, r, arg);
    }

    public void error(Reconciliation r, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.error(r.getMarker(), "{}: " + msg, r, arg1, arg2);
    }

    public void error(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.error(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3);
    }

    // TRACE
    public void trace(Reconciliation r, String msg) {
        this.log4j2Logger.trace(r.getMarker(), "{}: " + msg, r);
    }

    public void trace(Reconciliation r, String msg, Object arg) {
        this.log4j2Logger.trace(r.getMarker(), "{}: " + msg, r, arg);
    }

    public void trace(Reconciliation r, Object arg) {
        this.log4j2Logger.trace(r.getMarker(), "{}: {}", r, arg);
    }

    public void trace(Reconciliation r, String msg, Object arg1, Object arg2) {
        this.log4j2Logger.trace(r.getMarker(), "{}: " + msg, r, arg1, arg2);
    }

    public void trace(Reconciliation r, String msg, Object arg1, Object arg2, Object arg3) {
        this.log4j2Logger.trace(r.getMarker(), "{}: " + msg, r, arg1, arg2, arg3);
    }

}
