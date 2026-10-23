/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.common.Reconciliation;

/**
 * Helper interface to pass different KafkaConnectApi implementations
 */
public interface KafkaConnectApiProvider {
    /**
     * Creates an instance of KafkaConnectApi for the Connect cluster the reconciliation is for. The caller is
     * responsible for closing the returned client when it is not needed anymore.
     *
     * @param reconciliation    Reconciliation information
     *
     * @return  KafkaConnectApi instance
     */
    KafkaConnectApi createKafkaConnectApi(Reconciliation reconciliation);
}
