/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

// Original author: David Kornel

package io.strimzi.systemtest.timemeasuring;

public enum Operation {
    TEST_EXECUTION,
    CREATE_KAFKA,
    DELETE_KAFKA,
    UPDATE_KAFKA,
    CREATE_CLUSTER_OPERATOR,
    DELETE_CLUSTER_OPERATOR,
    CREATE_NAMESPACE,
    DELETE_NAMESPACE,
    CREATE_USER,
    DELETE_USER,
    UPDATE_USER,
    CREATE_SELENIUM_CONTAINER,
    DELETE_SELENIUM_CONTAINER,
}