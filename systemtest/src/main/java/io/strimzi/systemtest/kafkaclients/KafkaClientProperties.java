/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.executor.Exec;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
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
import java.util.Base64;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Class KafkaClientProperties, which holds inner class builder for fluent way to invoke objects. It is used inside
 * all our external clients such as BasicExternalKafkaClient or OauthExternalKafkaClient.
 *
 * @see io.strimzi.systemtest.kafkaclients.externalClients.OauthExternalKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.externalClients.BasicExternalKafkaClient
 * @see io.strimzi.systemtest.kafkaclients.externalClients.TracingExternalKafkaClient
 */
//  This practically means, always make sure that before invoking this withSharedProperties(), you need first execute withCaSecretName().
@SuppressFBWarnings({"NP_NONNULL_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR", "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"})
public class KafkaClientProperties  {

    private static final Logger LOGGER = LogManager.getLogger(KafkaClientProperties.class);

    private String namespaceName;
    private String clusterName;
    private String caSecretName;
    private String kafkaUsername;
    private Properties properties;

    public static class KafkaClientPropertiesBuilder {

        private static final String TRUSTSTORE_TYPE_CONFIG = "PKCS12";

        private Properties properties = new Properties();
        private String namespaceName;
        private String clusterName;
        private String caSecretName;
        private String kafkaUsername = "";

        public KafkaClientPropertiesBuilder withBootstrapServerConfig(String bootstrapServer) {

            this.properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            return this;
        }

        public KafkaClientPropertiesBuilder withKeySerializerConfig(Class<? extends Serializer> keySerializer) {

            this.properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
            return this;
        }

        public KafkaClientPropertiesBuilder withKeyDeserializerConfig(Class<? extends Deserializer> keyDeSerializer) {

            this.properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeSerializer.getName());
            return this;
        }

        public KafkaClientPropertiesBuilder withValueSerializerConfig(Class<? extends Serializer> valueSerializer) {

            this.properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
            return this;
        }

        public KafkaClientPropertiesBuilder withValueDeserializerConfig(Class<? extends Deserializer> valueDeSerializer) {

            this.properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeSerializer.getName());
            return this;
        }

        public KafkaClientPropertiesBuilder withMaxBlockMsConfig(String maxBlockMsConfig) {

            this.properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMsConfig);
            return this;
        }

        public KafkaClientPropertiesBuilder withClientIdConfig(String clientId) {

            this.properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            return this;
        }

        public KafkaClientPropertiesBuilder withAcksConfig(String acksConfig) {

            this.properties.setProperty(ProducerConfig.ACKS_CONFIG, acksConfig);
            return this;
        }

        public KafkaClientPropertiesBuilder withGroupIdConfig(String groupIdConfig) {

            this.properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdConfig);
            return this;
        }

        @SuppressWarnings("Regexp") // for the `.toLowerCase()` because kafka needs this property as lower-case
        @SuppressFBWarnings("DM_CONVERT_CASE")
        public KafkaClientPropertiesBuilder withAutoOffsetResetConfig(OffsetResetStrategy offsetResetConfig) {

            this.properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetConfig.name().toLowerCase());
            return this;
        }

        public KafkaClientPropertiesBuilder withNamespaceName(String namespaceName) {

            this.namespaceName = namespaceName;
            return this;
        }

        public KafkaClientPropertiesBuilder withClusterName(String clusterName) {

            this.clusterName = clusterName;
            return this;
        }

        public KafkaClientPropertiesBuilder withCaSecretName(String caSecretName) {

            this.caSecretName = caSecretName;
            return this;
        }

        public KafkaClientPropertiesBuilder withKafkaUsername(String kafkaUsername) {

            this.kafkaUsername = kafkaUsername;
            return this;
        }

        public KafkaClientPropertiesBuilder withSecurityProtocol(SecurityProtocol securityProtocol) {

            this.properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);
            return this;
        }

        public KafkaClientPropertiesBuilder withSaslMechanism(String saslMechanismType) {

            this.properties.setProperty(SaslConfigs.SASL_MECHANISM, saslMechanismType);
            return this;
        }

        // oauth properties


        public KafkaClientPropertiesBuilder withSaslLoginCallbackHandlerClass() {

            this.properties.setProperty(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
            return this;
        }

        public KafkaClientPropertiesBuilder withSaslJassConfig(String clientId, String clientSecretName, String oauthTokenEndpointUri) {
            if (clientId.isEmpty() || clientSecretName.isEmpty() || oauthTokenEndpointUri.isEmpty()) {
                throw new InvalidParameterException("You do not specify client-id, client-secret name or oauth-token-endpoint-uri inside kafka client!");
            }

            this.properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule " +
                    "required " +
                    "oauth.client.id=\"" + clientId + "\" " +
                    "oauth.client.secret=\"" + clientSecretName + "\" " +
                    "oauth.token.endpoint.uri=\"" + oauthTokenEndpointUri + "\";");

            return this;
        }

        public KafkaClientPropertiesBuilder withSaslJassConfigAndTls(String clientId, String clientSecretName, String oauthTokenEndpointUri) {

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

            return this;
        }

        /**
         * Create properties which are same pro producer and consumer
         */
        public KafkaClientPropertiesBuilder withSharedProperties() {
            // For turn off hostname verification
            properties.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

            try {
                if (!properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equals(CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL) &&
                    !properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equals("SASL_" + CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL)
                ) {
                    Secret clusterCaCertSecret = kubeClient(namespaceName).getSecret(caSecretName);
                    File tsFile = File.createTempFile(KafkaClientProperties.class.getName(), ".truststore");
                    tsFile.deleteOnExit();
                    KeyStore ts = KeyStore.getInstance(TRUSTSTORE_TYPE_CONFIG);
                    String tsPassword = "foo";
                    if (caSecretName.contains("custom-certificate")) {
                        ts.load(null, tsPassword.toCharArray());
                        CertificateFactory cf = CertificateFactory.getInstance("X.509");
                        String clusterCaCert = kubeClient(namespaceName).getSecret(caSecretName).getData().get("ca.crt");
                        Certificate cert = cf.generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(clusterCaCert)));
                        ts.setCertificateEntry("ca.crt", cert);
                        try (FileOutputStream tsOs = new FileOutputStream(tsFile)) {
                            ts.store(tsOs, tsPassword.toCharArray());
                        }
                    } else {
                        tsPassword = new String(Base64.getDecoder().decode(clusterCaCertSecret.getData().get("ca.password")), StandardCharsets.US_ASCII);
                        String truststore = clusterCaCertSecret.getData().get("ca.p12");
                        Files.write(tsFile.toPath(), Base64.getDecoder().decode(truststore));
                    }
                    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, ts.getType());
                    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, tsPassword);
                    properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, tsFile.getAbsolutePath());
                }

                if (!kafkaUsername.isEmpty()
                    && properties.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG).equals(SecurityProtocol.SASL_SSL.name)
                    && !properties.getProperty(SaslConfigs.SASL_MECHANISM).equals("OAUTHBEARER")) {

                    properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
                    Secret userSecret = kubeClient(namespaceName).getSecret(kafkaUsername);
                    String password = new String(Base64.getDecoder().decode(userSecret.getData().get("password")), StandardCharsets.UTF_8);

                    String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
                    String jaasCfg = String.format(jaasTemplate, kafkaUsername, password);

                    properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasCfg);
                } else if (!kafkaUsername.isEmpty()) {

                    Secret userSecret = kubeClient(namespaceName).getSecret(kafkaUsername);

                    String clientsCaCert = userSecret.getData().get("ca.crt");
                    LOGGER.debug("Clients CA cert: {}", clientsCaCert);

                    String userCaCert = userSecret.getData().get("user.crt");
                    String userCaKey = userSecret.getData().get("user.key");
                    String ksPassword = "foo";
                    properties.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, ksPassword);
                    LOGGER.debug("User CA cert: {}", userCaCert);
                    LOGGER.debug("User CA key: {}", userCaKey);
                    File ksFile = createKeystore(Base64.getDecoder().decode(clientsCaCert),
                        Base64.getDecoder().decode(userCaCert),
                        Base64.getDecoder().decode(userCaKey),
                        ksPassword);
                    properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ksFile.getAbsolutePath());
                    properties.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, TRUSTSTORE_TYPE_CONFIG);
                }
            } catch (RuntimeException | IOException | KeyStoreException | CertificateException | NoSuchAlgorithmException | InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }

            return this;
        }

        public KafkaClientProperties build() {

            return new KafkaClientProperties(this);
        }
    }

    private KafkaClientProperties(KafkaClientPropertiesBuilder builder) {

        this.properties = builder.properties;
        this.caSecretName = builder.caSecretName;
        this.kafkaUsername = builder.kafkaUsername;
        this.namespaceName = builder.namespaceName;
        this.clusterName = builder.clusterName;
    }

    /**
     * Create keystore
     * @param ca certificate authority
     * @param cert certificate
     * @param key key
     * @param password password
     * @return keystore location as File
     * @throws IOException
     * @throws InterruptedException
     */
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    private static File createKeystore(byte[] ca, byte[] cert, byte[] key, String password) throws IOException, InterruptedException {

        File caFile = File.createTempFile(KafkaClientProperties.class.getName(), ".crt");
        caFile.deleteOnExit();
        Files.write(caFile.toPath(), ca);
        File certFile = File.createTempFile(KafkaClientProperties.class.getName(), ".crt");
        certFile.deleteOnExit();
        Files.write(certFile.toPath(), cert);
        File keyFile = File.createTempFile(KafkaClientProperties.class.getName(), ".key");
        keyFile.deleteOnExit();
        Files.write(keyFile.toPath(), key);
        File keystore = File.createTempFile(KafkaClientProperties.class.getName(), ".keystore");
        keystore.delete(); // Note horrible race condition, but this is only for testing
        // RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -name $HOSTNAME -password pass:$2 -out $1
        if (new ProcessBuilder("openssl",
                "pkcs12",
                "-export",
                "-in", certFile.getAbsolutePath(),
                "-inkey", keyFile.getAbsolutePath(),
                "-chain",
                "-CAfile", caFile.getAbsolutePath(),
                "-name", "dfbdbd",
                "-password", "pass:" + password,
                "-out", keystore.getAbsolutePath()).inheritIO().start().waitFor() != 0) {
            fail();
        }
        keystore.deleteOnExit();
        return keystore;
    }

    private static void importKeycloakCertificateToTruststore(Properties clientProperties) throws IOException {

        String responseKeycloak = Exec.exec("openssl", "s_client", "-showcerts", "-connect",
            ResourceManager.kubeClient().getNodeAddress() + ":" + Constants.HTTPS_KEYCLOAK_DEFAULT_NODE_PORT).out();
        Matcher matcher = Pattern.compile("-----(?s)(.*)-----").matcher(responseKeycloak);

        if (matcher.find()) {
            String keycloakCertificateData = matcher.group(0);
            LOGGER.info("Keycloak cert is:{}\n", keycloakCertificateData);

            LOGGER.info("Creating keycloak.crt file");
            File keycloakCertFile = File.createTempFile("keycloak", ".crt");
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



}
