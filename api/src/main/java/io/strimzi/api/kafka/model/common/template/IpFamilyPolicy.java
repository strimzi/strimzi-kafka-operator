/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.common.template;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

public enum IpFamilyPolicy {
    SINGLE_STACK,
    PREFER_DUAL_STACK,
    REQUIRE_DUAL_STACK;

    @JsonCreator
    public static IpFamilyPolicy forValue(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "singlestack":
                return SINGLE_STACK;
            case "preferdualstack":
                return PREFER_DUAL_STACK;
            case "requiredualstack":
                return REQUIRE_DUAL_STACK;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case SINGLE_STACK:
                return "SingleStack";
            case PREFER_DUAL_STACK:
                return "PreferDualStack";
            case REQUIRE_DUAL_STACK:
                return "RequireDualStack";
            default:
                return null;
        }
    }
}
