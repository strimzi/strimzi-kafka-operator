/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.TlsPemIdentity;

/**
 * Class to provide the real KafkaAgentClient which connects to actual Kafka Agent
 */
public class DefaultKafkaAgentClientProvider implements KafkaAgentClientProvider {

    @Override
    public KafkaAgentClient createKafkaAgentClient(Reconciliation reconciliation, TlsPemIdentity tlsPemIdentity) {
        return new KafkaAgentClient(reconciliation, reconciliation.name(), reconciliation.namespace(), tlsPemIdentity);
    }
}
