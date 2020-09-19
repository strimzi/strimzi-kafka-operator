/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.Ca;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
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
        Admin ac;
        String trustStorePassword = null;
        File truststoreFile = null;
        // provided Secret with cluster CA certificate for TLS encryption
        if (clusterCaCertSecret != null) {
            PasswordGenerator pg = new PasswordGenerator(12);
            trustStorePassword = pg.generate();
            truststoreFile = Util.createFileTrustStore(getClass().getName(), "ts", Ca.cert(clusterCaCertSecret, Ca.CA_CRT), trustStorePassword.toCharArray());
        }

        try {
            String keyStorePassword = null;
            File keystoreFile = null;
            // provided Secret and related key for getting the private key for TLS client authentication
            if (keyCertSecret != null && keyCertName != null && !keyCertName.isEmpty()) {
                keyStorePassword = new String(Util.decodeFromSecret(keyCertSecret, keyCertName + ".password"), StandardCharsets.US_ASCII);
                keystoreFile = Util.createFileStore(getClass().getName(), "ts", Util.decodeFromSecret(keyCertSecret, keyCertName + ".p12"));
            }

            try {
                Properties p = new Properties();
                p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapHostnames);

                // configuring TLS encryption if requested
                if (truststoreFile != null && trustStorePassword != null) {
                    p.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");

                    p.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreFile.getAbsolutePath());
                    p.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
                    p.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
                }

                // configuring TLS client authentication
                if (keystoreFile != null && keyStorePassword != null) {
                    p.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreFile.getAbsolutePath());
                    p.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
                    p.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
                }

                p.setProperty(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "30000");

                ac = Admin.create(p);
            } finally {
                if (keystoreFile != null && !keystoreFile.delete()) {
                    LOGGER.warn("Unable to delete keystore file {}", keystoreFile);
                }
            }
        } finally {
            if (truststoreFile != null && !truststoreFile.delete()) {
                LOGGER.warn("Unable to delete truststore file {}", truststoreFile);
            }
        }
        return ac;
    }
}
