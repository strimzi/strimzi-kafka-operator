/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect.build;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Locale;

/**
 * Enum defining the supported Maven dependency include scopes for dependency
 * inclusion
 */
public enum MavenArtifactIncludeScope {
    COMPILE,
    PROVIDED,
    RUNTIME,
    TEST,
    SYSTEM;

    @JsonCreator
    public static MavenArtifactIncludeScope forValue(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "compile":
                return COMPILE;
            case "provided":
                return PROVIDED;
            case "runtime":
                return RUNTIME;
            case "test":
                return TEST;
            case "system":
                return SYSTEM;
            default:
                return null;
        }
    }

    @JsonValue
    public String toValue() {
        switch (this) {
            case COMPILE:
                return "compile";
            case PROVIDED:
                return "provided";
            case RUNTIME:
                return "runtime";
            case TEST:
                return "test";
            case SYSTEM:
                return "system";
            default:
                return null;
        }
    }
}