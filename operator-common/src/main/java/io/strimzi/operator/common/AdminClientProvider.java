/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import org.apache.kafka.clients.admin.Admin;

import java.util.Properties;

/**
 * Interface to be implemented for returning an instance of Kafka Admin interface
 */
public interface AdminClientProvider {

    /**
     * Create a Kafka Admin interface instance for brokers
     *
     * @param bootstrapHostnames Kafka hostname to connect to for administration operations
     * @param kafkaCaTrustSet Trust set for connecting to Kafka
     * @param authIdentity Identity for TLS client authentication for connecting to Kafka
     * @return Instance of Kafka Admin interface
     */
    Admin createAdminClient(String bootstrapHostnames, PemTrustSet kafkaCaTrustSet, PemAuthIdentity authIdentity);

    /**
     * Create a Kafka Admin interface instance for controllers
     *
     * @param controllerBootstrapHostnames Kafka controller hostname to connect to for administration operations
     * @param kafkaCaTrustSet Trust set for connecting to Kafka
     * @param authIdentity Identity for TLS client authentication for connecting to Kafka
     * @return Instance of Kafka Admin interface
     */
    Admin createControllerAdminClient(String controllerBootstrapHostnames, PemTrustSet kafkaCaTrustSet, PemAuthIdentity authIdentity);

    /**
     * Create a Kafka Admin interface instance for brokers
     *
     * @param bootstrapHostnames Kafka hostname to connect to for administration operations
     * @param kafkaCaTrustSet Trust set for connecting to Kafka
     * @param authIdentity Identity for TLS client authentication for connecting to Kafka
     * @param config Additional configuration for the Kafka Admin Client
     *
     * @return Instance of Kafka Admin interface
     */
    Admin createAdminClient(String bootstrapHostnames, PemTrustSet kafkaCaTrustSet, PemAuthIdentity authIdentity, Properties config);

    /**
     * Create a Kafka Admin interface instance for controllers
     *
     * @param controllerBootstrapHostnames Kafka hostname to connect to for administration operations
     * @param kafkaCaTrustSet Trust set for connecting to Kafka
     * @param authIdentity Identity for TLS client authentication for connecting to Kafka
     * @param config Additional configuration for the Kafka Admin Client
     *
     * @return Instance of Kafka Admin interface
     */
    Admin createControllerAdminClient(String controllerBootstrapHostnames, PemTrustSet kafkaCaTrustSet, PemAuthIdentity authIdentity, Properties config);
}
