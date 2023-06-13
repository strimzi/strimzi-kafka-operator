/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BrokerState {
    private static final int BROKER_RECOVERY_STATE = 2;

    private int code;

    private Map<String, Object> recoveryState;

    @JsonCreator
    public BrokerState(@JsonProperty("brokerState")int code, @JsonProperty("recoveryState") Map<String, Object> recoveryState) {
        this.code = code;
        this.recoveryState = recoveryState;
    }

    public int code() {
        return code;
    }

    public int remainingLogsToRecover() {
        if (recoveryState != null  && recoveryState.containsKey("remainingLogsToRecover")) {
            return (int) recoveryState.get("remainingLogsToRecover");
        }
        return 0;
    }

    public int remainingSegmentsToRecover() {
        if (recoveryState != null  && recoveryState.containsKey("remainingSegmentsToRecover")) {
            return (int) recoveryState.get("remainingSegmentsToRecover");
        }
        return 0;
    }

    @Override
    public String toString() {
        return String.format("Broker state: {} , Recovery state: {}", code, recoveryState.toString());
    }

    public boolean isBrokerInRecovery() {
        return code == BROKER_RECOVERY_STATE;
    }
}
