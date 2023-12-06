/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.user.acl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum AclRuleType {
    ALLOW,
    DENY;

    @JsonCreator
    public static AclRuleType forValue(String value) {
        switch (value) {
            case "allow":
                return ALLOW;
            case "deny":
                return DENY;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case ALLOW:
                return "allow";
            case DENY:
                return "deny";
            default:
                return null;
        }
    }
}
