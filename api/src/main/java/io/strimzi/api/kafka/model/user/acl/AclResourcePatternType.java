/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user.acl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AclResourcePatternType {
    LITERAL,
    PREFIX;

    @JsonCreator
    public static AclResourcePatternType forValue(String value) {
        switch (value) {
            case "literal":
                return LITERAL;
            case "prefix":
                return PREFIX;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case LITERAL:
                return "literal";
            case PREFIX:
                return "prefix";
            default:
                return null;
        }
    }
}
