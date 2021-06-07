/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.Secret;
import org.apache.kafka.clients.admin.Admin;

/**
 * Interface to be implemented for returning an instance of Kafka Admin interface
 */
public interface AdminClientProvider {

    /**
     * Create a Kafka Admin interface instance
     *
     * @param bootstrapHostnames Kafka hostname to connect to for administration operations
     * @param clusterCaCertSecret Secret containing the cluster CA certificate for TLS encryption
     * @param keyCertSecret Secret containing keystore for TLS client authentication
     * @param keyCertName Key inside the keyCertSecret for getting the keystore and the corresponding password
     * @return Instance of Kafka Admin interface
     */
    Admin createAdminClient(String bootstrapHostnames, Secret clusterCaCertSecret, Secret keyCertSecret, String keyCertName);
}
