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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;

public class DefaultAdminClientProvider implements AdminClientProvider {

    private static final Logger LOGGER = LogManager.getLogger(DefaultAdminClientProvider.class);

    @Override
    public Admin createAdminClient(String hostname, Secret clusterCaCertSecret, Secret keyCertSecret, String keyCertName) {

        Admin ac;
        String trustStorePassword = new String(decodeFromSecret(clusterCaCertSecret, Ca.CA_STORE_PASSWORD), StandardCharsets.US_ASCII);
        File truststoreFile = createFileStore(decodeFromSecret(clusterCaCertSecret, Ca.CA_STORE));
        try {
            String keyStorePassword = new String(decodeFromSecret(keyCertSecret, keyCertName + ".password"), StandardCharsets.US_ASCII);
            File keystoreFile = createFileStore(decodeFromSecret(keyCertSecret, keyCertName + ".p12"));
            try {
                Properties p = new Properties();
                p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, hostname);
                p.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");

                p.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreFile.getAbsolutePath());
                p.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
                p.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);

                p.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreFile.getAbsolutePath());
                p.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
                p.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
                p.setProperty(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "30000");

                ac = Admin.create(p);
            } finally {
                if (!keystoreFile.delete()) {
                    LOGGER.warn("Unable to delete keystore file {}", keystoreFile);
                }
            }
        } finally {
            if (!truststoreFile.delete()) {
                LOGGER.warn("Unable to delete truststore file {}", truststoreFile);
            }
        }
        return ac;
    }

    private File createFileStore(byte[] bytes) {
        File f = null;
        try {
            f = File.createTempFile(getClass().getName(), "ts");
            f.deleteOnExit();
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(f))) {
                os.write(bytes);
            }
            return f;
        } catch (IOException e) {
            if (f != null && !f.delete()) {
                LOGGER.warn("Failed to delete temporary file in exception handler");
            }
            throw new RuntimeException(e);
        }
    }

    private static byte[] decodeFromSecret(Secret secret, String key) {
        return Base64.getDecoder().decode(secret.getData().get(key));
    }
}
