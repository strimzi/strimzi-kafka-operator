/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.enums;

/**
 * Enum for determining in which mode we would like to run the Kafka and KafkaNodePools
 * In case of {@link #SEPARATE}, we have different NodePool for "broker" role and for "controller" role
 * In {@link #MIXED} case, there is one and only NodePool with both "broker" and "controller" roles
 */
public enum NodePoolsRoleMode {
    SEPARATE,
    MIXED
}
