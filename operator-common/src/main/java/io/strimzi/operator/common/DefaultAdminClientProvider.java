/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.strimzi.operator.common.auth.AuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.TrustSet;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

/**
 * Provides the default Kafka Admin client
 */
public class DefaultAdminClientProvider implements AdminClientProvider {
    /**
     * Constructor
     */
    public DefaultAdminClientProvider() { }

    @Override
    public Admin createAdminClient(String bootstrapHostnames, TrustSet kafkaTrustSet, AuthIdentity authIdentity) {
        return createAdminClient(bootstrapHostnames, kafkaTrustSet, authIdentity, new Properties());
    }

    @Override
    public Admin createControllerAdminClient(String controllerBootstrapHostnames, TrustSet kafkaTrustSet, AuthIdentity authIdentity) {
        return createControllerAdminClient(controllerBootstrapHostnames, kafkaTrustSet, authIdentity, new Properties());
    }

    /**
     * Create a Kafka Admin client.
     *
     * The {@code kafkaTrustSet} control the encryption of the connection. The {@code authIdentity} controls the
     * authentication. If {@code kafkaTrustSet} is null, the connection will be plaintext. If {@code kafkaTrustSet} is
     * not null, the connection will be TLS encrypted. If {@code authIdentity} is null, there will be no authentication.
     * If {@code authIdentity} is not null, the client will use the provided identity for authentication. The identity
     * might currently correspond to mTLS or Service-Account-based authentication.
     *
     * @param bootstrapHostnames    Hostnames of the Kafka bootstrap servers
     * @param kafkaTrustSet         Trust set for connecting to Kafka
     * @param authIdentity          Identity for authentication for connecting to Kafka
     * @param config                Custom Admin client configuration or empty properties instance
     *
     * @return  Admin client instance
     */
    @Override
    public Admin createAdminClient(String bootstrapHostnames, TrustSet kafkaTrustSet, AuthIdentity authIdentity, Properties config) {
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapHostnames);
        return Admin.create(adminClientConfiguration(kafkaTrustSet, authIdentity, config));
    }

    @Override
    public Admin createControllerAdminClient(String controllerBootstrapHostnames, TrustSet kafkaTrustSet, AuthIdentity authIdentity, Properties config) {
        config.setProperty(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, controllerBootstrapHostnames);
        return Admin.create(adminClientConfiguration(kafkaTrustSet, authIdentity, config));
    }

    /**
     * Utility method for preparing the Admin client configuration
     *
     * @param kafkaTrustSet         Trust set for connecting to Kafka
     * @param authIdentity          Identity for authentication for connecting to Kafka
     * @param config                Custom Admin client configuration or empty properties instance
     *
     * @return  Admin client configuration
     */
    /* test */ Properties adminClientConfiguration(TrustSet kafkaTrustSet, AuthIdentity authIdentity, Properties config)    {
        if (config == null) {
            throw new InvalidConfigurationException("The config parameter should not be null");
        }

        // configuring TLS encryption if requested
        if (kafkaTrustSet instanceof PemTrustSet pemTrustSet) {
            config.putIfAbsent(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
            config.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            config.setProperty(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, pemTrustSet.trustedCertificatesString());
        }

        // Configure the authentication
        if (authIdentity != null) {
            // We update the security protocol if needed
            if (authIdentity.isSasl())  {
                config.compute(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, (k, v) -> "SSL".equals(v) ? "SASL_SSL" : "SASL_PLAINTEXT");
            }

            config.putAll(authIdentity.kafkaClientProperties());
        }

        config.putIfAbsent(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "30000");
        config.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        config.putIfAbsent(AdminClientConfig.RETRIES_CONFIG, "3");
        config.putIfAbsent(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "40000");

        return config;
    }
}
