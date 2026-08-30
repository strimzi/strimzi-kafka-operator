/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.Identity;

/**
 * Class to provide the real KafkaAgentClient which connects to actual Kafka Agent
 */
public class DefaultKafkaAgentClientProvider implements KafkaAgentClientProvider {
    private final KubernetesClient kubernetesClient;

    /**
     * Constructor
     *
     * @param kubernetesClient  Kubernetes client to interact with the Kubernetes API
     */
    public DefaultKafkaAgentClientProvider(KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
    }

    @Override
    public KafkaAgentClient createKafkaAgentClient(Reconciliation reconciliation, Identity identity) {
        return new KafkaAgentClient(reconciliation, reconciliation.name(), reconciliation.namespace(), identity, kubernetesClient);
    }
}
