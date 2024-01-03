/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.PemAuthIdentity;
import io.strimzi.operator.common.model.PemTrustSet;

/**
 * Helper interface to pass different KafkaAgentClient implementations
 */
public interface KafkaAgentClientProvider {

    /**
     * Creates an instance of KafkaAgentClient
     *
     * @param reconciliation    Reconciliation information
     * @param pemTrustSet Trust set for TLS encryption
     * @param pemAuthIdentity Identity for TLS client authentication
     *
     * @return  KafkaAgentClient instance
     */
    KafkaAgentClient createKafkaAgentClient(Reconciliation reconciliation, PemTrustSet pemTrustSet, PemAuthIdentity pemAuthIdentity);
}
