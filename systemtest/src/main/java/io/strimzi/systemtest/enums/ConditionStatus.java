/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.enums;

/**
 * Status of the CR, found inside .status.conditions.*.status
 */
public enum ConditionStatus {
    True,
    False
}
