/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.common.BackOff;
import io.vertx.core.Promise;

import java.util.function.Supplier;

/**
 * Contains state, flags as to what kind of restart (if any) to do, and reasons why
 */
class RestartContext {
    private final Promise<Void> promise;
    private final BackOff backOff;
    private long connectionErrorStart = 0L;

    private boolean needsRestart;
    private boolean needsReconfig;
    private boolean forceRestart;
    private RestartReasons restartReasons;
    private KafkaBrokerConfigurationDiff diff;
    private KafkaBrokerLoggingConfigurationDiff logDiff;

    RestartContext(Supplier<BackOff> backOffSupplier) {
        promise = Promise.promise();
        backOff = backOffSupplier.get();
        restartReasons = RestartReasons.empty();

        //Looks like this is deliberate to consume the first backoff delay of 0 by side effect
        getBackOff().delayMs();
    }

    public void clearConnectionError() {
        setConnectionErrorStart(0L);
    }

    long connectionError() {
        return getConnectionErrorStart();
    }

    void noteConnectionError() {
        if (getConnectionErrorStart() == 0L) {
            setConnectionErrorStart(System.currentTimeMillis());
        }
    }

    @Override
    public String toString() {
        return "RestartContext{" +
                "promise=" + promise +
                ", backOff=" + backOff +
                ", connectionErrorStart=" + connectionErrorStart +
                ", needsRestart=" + needsRestart +
                ", needsReconfig=" + needsReconfig +
                ", forceRestart=" + forceRestart +
                ", restartReasons=" + restartReasons +
                ", diff=" + diff +
                ", logDiff=" + logDiff +
                '}';
    }

    Promise<Void> getPromise() {
        return promise;
    }

    BackOff getBackOff() {
        return backOff;
    }

    long getConnectionErrorStart() {
        return connectionErrorStart;
    }

    void setConnectionErrorStart(long connectionErrorStart) {
        this.connectionErrorStart = connectionErrorStart;
    }

    boolean needsRestart() {
        return needsRestart;
    }

    void setNeedsRestart(boolean needsRestart) {
        this.needsRestart = needsRestart;
    }

    boolean needsReconfig() {
        return needsReconfig;
    }

    void setNeedsReconfig(boolean needsReconfig) {
        this.needsReconfig = needsReconfig;
    }

    boolean needsForceRestart() {
        return forceRestart;
    }

    void setForceRestart(boolean forceRestart) {
        this.forceRestart = forceRestart;
    }

    RestartReasons getRestartReasons() {
        return restartReasons;
    }

    void setRestartReasons(RestartReasons restartReasons) {
        this.restartReasons = restartReasons;
    }

    KafkaBrokerConfigurationDiff getDiff() {
        return diff;
    }

    void setDiff(KafkaBrokerConfigurationDiff diff) {
        this.diff = diff;
    }

    KafkaBrokerLoggingConfigurationDiff getLogDiff() {
        return logDiff;
    }

    void setLogDiff(KafkaBrokerLoggingConfigurationDiff logDiff) {
        this.logDiff = logDiff;
    }
}
