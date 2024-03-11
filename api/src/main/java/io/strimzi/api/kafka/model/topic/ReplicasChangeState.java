/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.topic;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * This defines values for KafkaTopic.status.replicasChange.state.
 */
public enum ReplicasChangeState {
    PENDING,
    ONGOING;

    @JsonCreator
    public static ReplicasChangeState forValue(String value) {
        switch (value) {
            case "pending":
                return PENDING;
            case "ongoing":
                return ONGOING;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case PENDING:
                return "pending";
            case ONGOING:
                return "ongoing";
            default:
                return null;
        }
    }
}
