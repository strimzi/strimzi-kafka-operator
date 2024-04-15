/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Enum that is used to define if the volume should be used to store the KRaft metadata. Currently, it has only two
 * options:
 * - shared for a volume that should share both KRaft metadata and regular logs
 * - Null for automatic assignment of the KRaft metadata to the volume with lowest ID
 */
public enum KRaftMetadataStorage {
    SHARED;

    @JsonCreator
    public static KRaftMetadataStorage forValue(String value) {
        //noinspection SwitchStatementWithTooFewBranches
        switch (value) {
            case "shared":
                return SHARED;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        //noinspection SwitchStatementWithTooFewBranches
        switch (this) {
            case SHARED:
                return "shared";
            default:
                return null;
        }
    }
}
