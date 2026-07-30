/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.operator.common.auth.AuthIdentity;
import io.strimzi.operator.common.auth.TrustSet;
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
     * @param kafkaTrustSet Trust set for connecting to Kafka
     * @param authIdentity Identity for authentication for connecting to Kafka
     * @return Instance of Kafka Admin interface
     */
    Admin createAdminClient(String bootstrapHostnames, TrustSet kafkaTrustSet, AuthIdentity authIdentity);

    /**
     * Create a Kafka Admin interface instance for controllers
     *
     * @param controllerBootstrapHostnames Kafka controller hostname to connect to for administration operations
     * @param kafkaTrustSet Trust set for connecting to Kafka
     * @param authIdentity Identity for authentication for connecting to Kafka
     * @return Instance of Kafka Admin interface
     */
    Admin createControllerAdminClient(String controllerBootstrapHostnames, TrustSet kafkaTrustSet, AuthIdentity authIdentity);

    /**
     * Create a Kafka Admin interface instance for brokers
     *
     * @param bootstrapHostnames Kafka hostname to connect to for administration operations
     * @param kafkaTrustSet Trust set for connecting to Kafka
     * @param authIdentity Identity for authentication for connecting to Kafka
     * @param config Additional configuration for the Kafka Admin Client
     *
     * @return Instance of Kafka Admin interface
     */
    Admin createAdminClient(String bootstrapHostnames, TrustSet kafkaTrustSet, AuthIdentity authIdentity, Properties config);

    /**
     * Create a Kafka Admin interface instance for controllers
     *
     * @param controllerBootstrapHostnames Kafka hostname to connect to for administration operations
     * @param kafkaTrustSet Trust set for connecting to Kafka
     * @param authIdentity Identity for authentication for connecting to Kafka
     * @param config Additional configuration for the Kafka Admin Client
     *
     * @return Instance of Kafka Admin interface
     */
    Admin createControllerAdminClient(String controllerBootstrapHostnames, TrustSet kafkaTrustSet, AuthIdentity authIdentity, Properties config);
}
