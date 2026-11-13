/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.common.Reconciliation;

/**
 * Class to provide the real KafkaConnectApi which connects to the actual Kafka Connect REST API
 */
public class DefaultKafkaConnectApiProvider implements KafkaConnectApiProvider {
    /**
     * Constructor
     */
    public DefaultKafkaConnectApiProvider() { }

    @Override
    public KafkaConnectApi createKafkaConnectApi(Reconciliation reconciliation) {
        return new KafkaConnectApiImpl();
    }
}
