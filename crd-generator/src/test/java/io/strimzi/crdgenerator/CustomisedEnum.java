/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum CustomisedEnum {
    ONE,
    TWO;

    @JsonCreator
    public static CustomisedEnum forValue(String value) {
        switch (value) {
            case "one":
                return ONE;
            case "two":
                return TWO;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case ONE:
                return "one";
            case TWO:
                return "two";
            default:
                return null;
        }
    }

}
