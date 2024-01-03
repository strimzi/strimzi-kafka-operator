/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.operator.common.model.PemAuthIdentity;
import io.strimzi.operator.common.model.PemTrustSet;
import org.apache.kafka.clients.admin.Admin;

import java.util.Properties;

/**
 * Interface to be implemented for returning an instance of Kafka Admin interface
 */
public interface AdminClientProvider {

    /**
     * Create a Kafka Admin interface instance
     *
     * @param bootstrapHostnames Kafka hostname to connect to for administration operations
     * @param pemTrustSet Trust set for TLS encryption
     * @param pemAuthIdentity Identity for TLS client authentication
     * @return Instance of Kafka Admin interface
     */
    Admin createAdminClient(String bootstrapHostnames, PemTrustSet pemTrustSet, PemAuthIdentity pemAuthIdentity);

    /**
     * Create a Kafka Admin interface instance
     *
     * @param bootstrapHostnames Kafka hostname to connect to for administration operations
     * @param pemTrustSet Trust set for TLS encryption
     * @param pemAuthIdentity Identity for TLS client authentication
     * @param config Additional configuration for the Kafka Admin Client
     *
     * @return Instance of Kafka Admin interface
     */
    Admin createAdminClient(String bootstrapHostnames, PemTrustSet pemTrustSet, PemAuthIdentity pemAuthIdentity, Properties config);
}
