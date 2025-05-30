/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.clientproperties;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Secret;
import io.skodjob.testframe.executor.Exec;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.operator.common.Util;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.test.TestUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.InvalidParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class KafkaClientProperties, which holds inner class builder for fluent way to invoke objects. It is used inside
 * all our external clients such as ExternalKafkaClient.
 *
 * @see io.strimzi.systemtest.kafkaclients.externalClients.ExternalKafkaClient
 */
//  This practically means, always make sure that before invoking this withSharedProperties(), you need first execute withCaSecretName().
@SuppressFBWarnings({"NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"})
abstract public class AbstractKafkaClientProperties<C extends AbstractKafkaClientProperties<C>>  {

    private static final Logger LOGGER = LogManager.getLogger(AbstractKafkaClientProperties.class);

    private String namespaceName;
    private String clusterName;
    private String caSecretName;
    private String kafkaUsername;
    protected Properties properties;

    public static abstract class KafkaClientPropertiesBuilder<T extends AbstractKafkaClientProperties.KafkaClientPropertiesBuilder<T>> {

        private static final String TRUSTSTORE_TYPE_CONFIG = "PKCS12";

        protected Properties properties = new Properties();
        private String namespaceName;
        private String clusterName;
        private String caSecretName;
        private String kafkaUsername = "";

        public T withNamespaceName(String namespaceName) {

            this.namespaceName = namespaceName;
            return self();
        }

        public T withClusterName(String clusterName) {

            this.clusterName = clusterName;
            return self();
        }

        public T withCaSecretName(String caSecretName) {

            this.caSecretName = caSecretName;
            return self();
        }

        public T withKafkaUsername(String kafkaUsername) {

            this.kafkaUsername = kafkaUsername;
            return self();
        }

        public T withSecurityProtocol(SecurityProtocol securityProtocol) {

            this.properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
            return self();
        }

        public T withSaslMechanism(String saslMechanismType) {

            this.properties.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanismType);
            return self();
        }

        // oauth properties

        public T withSaslLoginCallbackHandlerClass() {

            this.properties.setProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
            return self();
        }

        public T withSaslJassConfig(String clientId, String clientSecretName, String oauthTokenEndpointUri) {
            if (clientId.isEmpty() || clientSecretName.isEmpty() || oauthTokenEndpointUri.isEmpty()) {
                throw new InvalidParameterException("You do not specify client-id, client-secret name or oauth-token-endpoint-uri inside Kafka client!");
            }

            this.properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule " +
                    "required " +
                    "oauth.client.id=\"" + clientId + "\" " +
                    "oauth.client.secret=\"" + clientSecretName + "\" " +
                    "oauth.token.endpoint.uri=\"" + oauthTokenEndpointUri + "\";");

            return self();
        }

        public T withSaslJassConfigAndTls(String clientId, String clientSecretName, String oauthTokenEndpointUri) {

            try {
                importKeycloakCertificateToTruststore(properties);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (clientId.isEmpty() || clientSecretName.isEmpty() || oauthTokenEndpointUri.isEmpty()) {
                throw new InvalidParameterException("You do not specify client-id, client-secret name or oauth-token-endpoint-uri inside kafka client!");
            }

            properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule " +
                    "required " +
                    "oauth.client.id=\"" + clientId + "\" " +
                    "oauth.client.secret=\"" + clientSecretName + "\" " +
                    "oauth.token.endpoint.uri=\"" + oauthTokenEndpointUri + "\" " +
                    "oauth.ssl.endpoint.identification.algorithm=\"\"" +
                    "oauth.ssl.truststore.location=\"" + properties.get(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG) + "\" " +
                    "oauth.ssl.truststore.password=\"" + properties.get(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG) + "\" " +
                    "oauth.ssl.truststore.type=\"" + properties.get(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG) + "\" ;");

            return self();
        }

        /**
         * Create properties which are same pro producer and consumer
         */
        public T withSharedProperties() {
            // For turn off hostname verification
            properties.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

            try {
                if (!properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equals(CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL) &&
                    !properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equals("SASL_" + CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL)
                ) {
                    Secret clusterCaCertSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(caSecretName).get();
                    File tsFile = Files.createTempFile(AbstractKafkaClientProperties.class.getName(), ".truststore").toFile();
                    tsFile.deleteOnExit();
                    KeyStore ts = KeyStore.getInstance(TRUSTSTORE_TYPE_CONFIG);
                    String tsPassword = "foo";
                    if (caSecretName.contains("custom-certificate")) {
                        ts.load(null, tsPassword.toCharArray());
                        CertificateFactory cf = CertificateFactory.getInstance("X.509");
                        String clusterCaCert = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(caSecretName).get().getData().get("ca.crt");
                        Certificate cert = cf.generateCertificate(new ByteArrayInputStream(Util.decodeBytesFromBase64(clusterCaCert)));
                        ts.setCertificateEntry("ca.crt", cert);
                        try (FileOutputStream tsOs = new FileOutputStream(tsFile)) {
                            ts.store(tsOs, tsPassword.toCharArray());
                        }
                    } else {
                        tsPassword = Util.decodeFromBase64(clusterCaCertSecret.getData().get("ca.password"));
                        String truststore = clusterCaCertSecret.getData().get("ca.p12");
                        Files.write(tsFile.toPath(), Util.decodeBytesFromBase64(truststore));
                    }
                    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, ts.getType());
                    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, tsPassword);
                    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, tsFile.getAbsolutePath());
                }

                if (!kafkaUsername.isEmpty()
                    && properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equals(SecurityProtocol.SASL_SSL.name)
                    && !properties.getProperty(SaslConfigs.SASL_MECHANISM).equals("OAUTHBEARER")) {

                    properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
                    Secret userSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(kafkaUsername).get();
                    String password = Util.decodeFromBase64(userSecret.getData().get("password"), StandardCharsets.UTF_8);

                    String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
                    String jaasCfg = String.format(jaasTemplate, kafkaUsername, password);

                    properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasCfg);
                } else if (!kafkaUsername.isEmpty()) {

                    Secret userSecret = KubeResourceManager.get().kubeClient().getClient().secrets().inNamespace(namespaceName).withName(kafkaUsername).get();

                    String clientsCaCert = userSecret.getData().get("ca.crt");
                    LOGGER.debug("Clients CA cert: {}", clientsCaCert);

                    String userCaCert = userSecret.getData().get("user.crt");
                    String userCaKey = userSecret.getData().get("user.key");
                    String ksPassword = "foo";
                    properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ksPassword);
                    LOGGER.debug("User CA cert: {}", userCaCert);
                    LOGGER.debug("User CA key: {}", userCaKey);
                    File ksFile = createKeystore(Util.decodeBytesFromBase64(clientsCaCert),
                        Util.decodeBytesFromBase64(userCaCert),
                        Util.decodeBytesFromBase64(userCaKey),
                        ksPassword);
                    properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ksFile.getAbsolutePath());
                    properties.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, TRUSTSTORE_TYPE_CONFIG);
                }
            } catch (RuntimeException | IOException | KeyStoreException | CertificateException | NoSuchAlgorithmException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }

            return self();
        }

        protected abstract AbstractKafkaClientProperties<?> build();

        // Subclasses must override this method to return "this" protected abstract T self();
        // for not explicit casting..
        protected abstract T self();
    }

    protected abstract KafkaClientPropertiesBuilder<?> toBuilder(C clientProperties) throws ClassNotFoundException;

    protected AbstractKafkaClientProperties(KafkaClientPropertiesBuilder<?> builder) {

        if (builder.namespaceName == null || builder.namespaceName.isEmpty()) throw new InvalidParameterException("Namespace name is not set.");
        if (builder.clusterName == null || builder.clusterName.isEmpty()) throw  new InvalidParameterException("Cluster name is not set.");
        if (builder.properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG) == null || builder.properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).isEmpty()) {
            throw new InvalidParameterException("Security protocol is not set.");
        }

        properties = builder.properties;
        caSecretName = builder.caSecretName;
        kafkaUsername = builder.kafkaUsername;
        namespaceName = builder.namespaceName;
        clusterName = builder.clusterName;
    }

    /**
     * Create keystore
     * @param ca certificate authority
     * @param cert certificate
     * @param key key
     * @param password password
     * @return keystore location as File
     * @throws IOException When opening or writing to file
     */
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private static File createKeystore(byte[] ca, byte[] cert, byte[] key, String password) throws IOException {

        File caFile = Files.createTempFile(AbstractKafkaClientProperties.class.getName(), ".crt").toFile();
        caFile.deleteOnExit();
        Files.write(caFile.toPath(), ca);
        File certFile = Files.createTempFile(AbstractKafkaClientProperties.class.getName(), ".crt").toFile();
        certFile.deleteOnExit();
        Files.write(certFile.toPath(), cert);
        File keyFile = Files.createTempFile(AbstractKafkaClientProperties.class.getName(), ".key").toFile();
        keyFile.deleteOnExit();
        Files.write(keyFile.toPath(), key);
        File keystore = Files.createTempFile(AbstractKafkaClientProperties.class.getName(), ".keystore").toFile();
        keystore.delete(); // Note horrible race condition, but this is only for testing
        // RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -name $HOSTNAME -password pass:$2 -out $1
        // The following code is needed to avoid race-condition which we see from time to time
        TestUtils.waitFor("client-keystore readiness", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.CO_OPERATION_TIMEOUT_MEDIUM,
            () -> Exec.exec("openssl",
                "pkcs12",
                "-export",
                "-in", certFile.getAbsolutePath(),
                "-inkey", keyFile.getAbsolutePath(),
                "-chain",
                "-CAfile", caFile.getAbsolutePath(),
                "-name", "dfbdbd",
                "-password", "pass:" + password,
                "-out", keystore.getAbsolutePath()).exitStatus());

        keystore.deleteOnExit();
        return keystore;
    }

    private static void importKeycloakCertificateToTruststore(Properties clientProperties) throws IOException {
        String nodeAddress = KubeResourceManager.get().kubeClient().getClient().nodes().list().getItems().get(0).getStatus().getAddresses().get(0).getAddress();
        String responseKeycloak = Exec.exec("openssl", "s_client", "-showcerts", "-connect",
            nodeAddress + ":" + TestConstants.HTTPS_KEYCLOAK_DEFAULT_NODE_PORT).out();
        Matcher matcher = Pattern.compile("-----(?s)(.*)-----").matcher(responseKeycloak);

        if (matcher.find()) {
            String keycloakCertificateData = matcher.group(0);
            LOGGER.info("Keycloak cert is: {}\n", keycloakCertificateData);

            LOGGER.info("Creating keycloak.crt file");
            File keycloakCertFile = Files.createTempFile("keycloak", ".crt").toFile();
            Files.write(keycloakCertFile.toPath(), keycloakCertificateData.getBytes(StandardCharsets.UTF_8));

            LOGGER.info("Importing keycloak certificate {} to truststore", keycloakCertFile.getAbsolutePath());
            Exec.exec("keytool", "-v", "-import", "-trustcacerts", "-file", keycloakCertFile.getAbsolutePath(),
                "-alias", "keycloakCrt1", "-keystore", clientProperties.get("ssl.truststore.location").toString(),
                "-noprompt", "-storepass", clientProperties.get("ssl.truststore.password").toString());
        }
    }

    public Properties getProperties() {
        return properties;
    }

    public String getNamespaceName() {
        return namespaceName;
    }
    public String getClusterName() {
        return clusterName;
    }
}
