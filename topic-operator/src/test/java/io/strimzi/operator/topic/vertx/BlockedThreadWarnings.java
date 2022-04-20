/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.vertx;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Used to share state between the BlockedThreadChecker wrapped logger and the unit test that needs this info.
 */
public class BlockedThreadWarnings {

    private static final BlockedThreadWarnings INSTANCE = new BlockedThreadWarnings();

    public static BlockedThreadWarnings getInstance() {
        return INSTANCE;
    }

    private final AtomicLong warningsLogged = new AtomicLong(0L);
    private final AtomicBoolean pickedUpByVertx = new AtomicBoolean(false);

    public void incrementWarning() {
        warningsLogged.incrementAndGet();
    }

    public long blockedThreadWarningsCount() {
        return warningsLogged.get();
    }

    public void pickedUpByVertx() {
        pickedUpByVertx.set(true);
    }

    public boolean isPickedUpByVertx() {
        return pickedUpByVertx.get();
    }

    public void reset() {
        warningsLogged.set(0);
    }

}
