/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.vertx;

import io.vertx.core.logging.SLF4JLogDelegate;
import io.vertx.core.logging.SLF4JLogDelegateFactory;
import io.vertx.core.spi.logging.LogDelegate;
import org.slf4j.LoggerFactory;

/**
 * Picked up by class name when Vertx starts up. Intercepts and wraps the BlockedThreadChecker's logger, so I can
 * verify whether it's picking up blocked threads or not. Believe it or not, this is the best way I've found of doing so.
 * ...eech.
 */
@SuppressWarnings("unused")
public class OverrideBlockedThreadCheckerLoggerDelegateFactory extends SLF4JLogDelegateFactory {

    @Override
    public LogDelegate createDelegate(String clazz) {
        if (clazz.endsWith("BlockedThreadChecker")) {
            BlockedThreadWarnings.getInstance().pickedUpByVertx();
            return new HackDelegate(clazz);
        } else {
            return super.createDelegate(clazz);
        }
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    static class HackDelegate extends SLF4JLogDelegate {
        public HackDelegate(Object logger) {
            super(LoggerFactory.getLogger(logger.toString()));
        }

        @Override
        public void warn(Object message) {
            BlockedThreadWarnings.getInstance().incrementWarning();
            super.warn(message);
        }

        @Override
        public void warn(Object message, Throwable t) {
            BlockedThreadWarnings.getInstance().incrementWarning();
            super.warn(message, t);
        }
    }
}
