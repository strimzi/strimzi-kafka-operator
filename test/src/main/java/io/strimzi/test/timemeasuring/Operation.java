/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.test.timemeasuring;

public enum Operation {
    TEST_EXECUTION,
    CLASS_EXECUTION,
    SCALE_UP,
    SCALE_DOWN,
    CO_CREATION,
    CO_DELETION,
    MM_DEPLOYMENT,
    CLUSTER_RECOVERY,
}