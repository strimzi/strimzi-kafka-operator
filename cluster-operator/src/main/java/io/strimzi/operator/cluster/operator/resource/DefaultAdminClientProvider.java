/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.PasswordGenerator;
import org.apache.kafka.clients.admin.AdminClient;
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
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class DefaultAdminClientProvider implements AdminClientProvider {

    private static final Logger LOGGER = LogManager.getLogger(DefaultAdminClientProvider.class);

    @Override
    public AdminClient createAdminClient(String hostname, Secret clusterCaCertSecret, Secret coKeySecret) {
        PasswordGenerator pg = new PasswordGenerator(12);
        AdminClient ac;
        String trustStorePassword = pg.generate();
        File truststoreFile = setupTrustStore(trustStorePassword.toCharArray(), Ca.cert(clusterCaCertSecret, Ca.CA_CRT));
        try {
            String keyStorePassword = pg.generate();
            File keystoreFile = setupKeyStore(coKeySecret,
                    keyStorePassword.toCharArray(),
                    Ca.cert(coKeySecret, "cluster-operator.crt"));
            try {
                Properties p = new Properties();
                p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, hostname);
                p.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
                p.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreFile.getAbsolutePath());
                p.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
                p.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
                p.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystoreFile.getAbsolutePath());
                p.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
                p.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyStorePassword);
                ac = AdminClient.create(p);
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

    private File setupKeyStore(Secret clusterSecretKey, char[] password,
                               X509Certificate clientCert) {
        Base64.Decoder decoder = Base64.getDecoder();

        try {
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, password);
            Pattern parse = Pattern.compile("^---*BEGIN.*---*$(.*)^---*END.*---*$.*", Pattern.MULTILINE | Pattern.DOTALL);

            String keyText = new String(decoder.decode(clusterSecretKey.getData().get("cluster-operator.key")), StandardCharsets.ISO_8859_1);
            Matcher matcher = parse.matcher(keyText);
            if (!matcher.find()) {
                throw new RuntimeException("Bad client (CO) key. Key misses BEGIN or END markers");
            }
            PrivateKey clientKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(
                    Base64.getMimeDecoder().decode(matcher.group(1))));

            keyStore.setEntry("cluster-operator",
                    new KeyStore.PrivateKeyEntry(clientKey, new Certificate[]{clientCert}),
                    new KeyStore.PasswordProtection(password));

            return store(password, keyStore);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private File setupTrustStore(char[] password, X509Certificate caCertCO) {
        try {
            KeyStore trustStore = null;
            trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, password);
            trustStore.setEntry(caCertCO.getSubjectDN().getName(), new KeyStore.TrustedCertificateEntry(caCertCO), null);
            return store(password, trustStore);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private File store(char[] password, KeyStore trustStore) throws Exception {
        File f = null;
        try {
            f = File.createTempFile(getClass().getName(), "ts");
            f.deleteOnExit();
            try (OutputStream os = new BufferedOutputStream(new FileOutputStream(f))) {
                trustStore.store(os, password);
            }
            return f;
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException | RuntimeException e) {
            if (f != null && !f.delete()) {
                LOGGER.warn("Failed to delete temporary file in exception handler");
            }
            throw e;
        }
    }
}
