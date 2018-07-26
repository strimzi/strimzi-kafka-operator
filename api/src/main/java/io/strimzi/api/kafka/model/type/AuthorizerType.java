/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AuthorizerType {
    SIMPLE_ACL_AUTHORIZER;

    @JsonCreator
    public static AuthorizerType forValue(String value) {
        switch (value) {
            case "SimpleAclAuthorizer":
                return SIMPLE_ACL_AUTHORIZER;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case SIMPLE_ACL_AUTHORIZER:
                return "SimpleAclAuthorizer";
            default:
                return null;
        }
    }
}
