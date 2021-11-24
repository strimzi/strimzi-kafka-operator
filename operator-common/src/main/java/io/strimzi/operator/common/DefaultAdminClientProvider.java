/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.Secret;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class DefaultAdminClientProvider implements AdminClientProvider {

    private static final Logger LOGGER = LogManager.getLogger(DefaultAdminClientProvider.class);

    /**
     * Create a Kafka Admin interface instance handling the following different scenarios:
     *
     * 1. No TLS connection, no TLS client authentication:
     *
     * If {@code clusterCaCertSecret}, {@code keyCertSecret} and {@code keyCertName} are null, the returned Admin Client instance
     * is configured to connect to the Apache Kafka bootstrap (defined via {@code hostname}) on plain connection with no
     * TLS encryption and no TLS client authentication.
     *
     * 2. TLS connection, no TLS client authentication
     *
     * If only {@code clusterCaCertSecret} is provided as not null, the returned Admin Client instance is configured to
     * connect to the Apache Kafka bootstrap (defined via {@code hostname}) on TLS encrypted connection but with no
     * TLS authentication.
     *
     * 3. TLS connection and TLS client authentication
     *
     * If {@code clusterCaCertSecret}, {@code keyCertSecret} and {@code keyCertName} are provided as not null, the returned
     * Admin Client instance is configured to connect to the Apache Kafka bootstrap (defined via {@code hostname}) on
     * TLS encrypted connection and with TLS client authentication.
     */
    @Override
    public Admin createAdminClient(String bootstrapHostnames, Secret clusterCaCertSecret, Secret keyCertSecret, String keyCertName) {
        Properties tlsProperties = Util.tlsProperties(clusterCaCertSecret, keyCertSecret, keyCertName);
        Properties properties = new Properties();

        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapHostnames);
        properties.setProperty(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "30000");
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.setProperty(AdminClientConfig.RETRIES_CONFIG, "3");
        properties.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "40000");

        properties.putAll(tlsProperties);
        return Admin.create(properties);
    }

    public Admin createAdminClient(Properties properties) {
        return Admin.create(properties);
    }
}
