/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.TlsPemIdentity;

/**
 * Helper interface to pass different KafkaAgentClient implementations
 */
public interface KafkaAgentClientProvider {

    /**
     * Creates an instance of KafkaAgentClient
     *
     * @param reconciliation    Reconciliation information
     * @param tlsPemIdentity    Trust set and identity for TLS client authentication for connecting to the Kafka cluster
     *
     * @return  KafkaAgentClient instance
     */
    KafkaAgentClient createKafkaAgentClient(Reconciliation reconciliation, TlsPemIdentity tlsPemIdentity);
}
