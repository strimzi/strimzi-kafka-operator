/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeAdminClientSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeConsumerSpec;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeHttpConfig;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeProducerSpec;
import io.strimzi.api.kafka.model.common.ClientTls;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.operator.common.Reconciliation;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * This class is used to generate the bridge configuration template. The template is later passed using a ConfigMap to
 * the bridge pod. The script in the container image will fill in the variables in the template and use the
 * configuration file. This class is using the builder pattern to make it easy to test the different parts etc. To
 * generate the configuration file, it is using the PrintWriter.
 */
public class KafkaBridgeConfigurationBuilder {

    // placeholders expanded through config providers inside the bridge node
    private static final String PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:CERTS_STORE_PASSWORD}";
    private static final String PLACEHOLDER_SASL_USERNAME_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:KAFKA_BRIDGE_SASL_USERNAME}";
    private static final String PASSWORD_VOLUME_MOUNT = "/opt/strimzi/bridge-password/";
    // the SASL password file template includes: <volume_mount>/<secret_name>/<password_file>
    private static final String PLACEHOLDER_SASL_PASSWORD_FILE_TEMPLATE_CONFIG_PROVIDER_DIR = "${strimzidir:%s%s:%s}";
    private static final String PLACEHOLDER_OAUTH_CONFIG_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:KAFKA_BRIDGE_OAUTH_CONFIG}";
    private static final String PLACEHOLDER_OAUTH_ACCESS_TOKEN_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:KAFKA_BRIDGE_OAUTH_ACCESS_TOKEN}";
    private static final String PLACEHOLDER_OAUTH_REFRESH_TOKEN_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:KAFKA_BRIDGE_OAUTH_REFRESH_TOKEN}";
    private static final String PLACEHOLDER_OAUTH_CLIENT_SECRET_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:KAFKA_BRIDGE_OAUTH_CLIENT_SECRET}";
    private static final String PLACEHOLDER_OAUTH_PASSWORD_GRANT_PASSWORD_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:KAFKA_BRIDGE_OAUTH_PASSWORD_GRANT_PASSWORD}";

    private final Reconciliation reconciliation;
    private final StringWriter stringWriter = new StringWriter();
    private final PrintWriter writer = new PrintWriter(stringWriter);

    private String securityProtocol = "PLAINTEXT";

    /**
     * Bridge configuration template constructor
     *
     * @param reconciliation    the reconciliation
     * @param bridgeId  the bridge ID
     * @param bootstrapServers  Kafka cluster bootstrap servers to connect to
     */
    public KafkaBridgeConfigurationBuilder(Reconciliation reconciliation, String bridgeId, String bootstrapServers) {
        this.reconciliation = reconciliation;
        printHeader();
        configureBridgeId(bridgeId);
        configureBootstrapServers(bootstrapServers);
    }

    /**
     * Renders the bridge ID configurations
     *
     * @param bridgeId  the bridge ID
     */
    private void configureBridgeId(String bridgeId)   {
        printSectionHeader("Bridge ID");
        writer.println("bridge.id=" + bridgeId);
        writer.println();
    }

    /**
     * Renders the Apache Kafka bootstrap servers configuration
     *
     * @param bootstrapServers  Kafka cluster bootstrap servers to connect to
     */
    private void configureBootstrapServers(String bootstrapServers) {
        printSectionHeader("Kafka bootstrap servers");
        writer.println("kafka.bootstrap.servers=" + bootstrapServers);
        writer.println();
    }

    /**
     * Configure the Kafka security protocol to be used
     * This internal method is used when the configuration is build, because the security protocol depends on
     * TLS and SASL authentication configurations and if they are set
     */
    private void configureSecurityProtocol() {
        printSectionHeader("Kafka Security protocol");
        writer.println("kafka.security.protocol=" + securityProtocol);
    }

    /**
     * Configures the Kafka config providers used for loading some parameters from env vars and files
     * (i.e. user and password for authentication)
     *
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withConfigProviders() {
        printSectionHeader("Config providers");
        writer.println("kafka.config.providers=strimzienv,strimzifile,strimzidir");
        writer.println("kafka.config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider");
        writer.println("kafka.config.providers.strimzienv.param.allowlist.pattern=.*");
        writer.println("kafka.config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider");
        writer.println("kafka.config.providers.strimzifile.param.allowed.paths=/opt/strimzi");
        writer.println("kafka.config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider");
        writer.println("kafka.config.providers.strimzidir.param.allowed.paths=/opt/strimzi");
        writer.println();
        return this;
    }

    /**
     * Adds the tracing type
     *
     * @param tracing   the tracing configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withTracing(Tracing tracing) {
        if (tracing != null) {
            printSectionHeader("Tracing configuration");
            writer.println("bridge.tracing=" + tracing.getType());
            writer.println();
        }
        return this;
    }

    /**
     * Adds the TLS/SSL configuration for the bridge to connect to the Kafka cluster.
     * The configuration includes the trusted certificates store for TLS connection (server authentication).
     *
     * @param tls   client TLS configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withTls(ClientTls tls) {
        if (tls != null) {
            securityProtocol = "SSL";

            if (tls.getTrustedCertificates() != null && !tls.getTrustedCertificates().isEmpty()) {
                printSectionHeader("TLS/SSL");
                writer.println("kafka.ssl.truststore.location=/tmp/strimzi/bridge.truststore.p12");
                writer.println("kafka.ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
                writer.println("kafka.ssl.truststore.type=PKCS12");
            }
        }
        return this;
    }

    /**
     * Add the SASL configuration for client authentication to the Kafka cluster
     *
     * @param authentication authentication configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withAuthentication(KafkaClientAuthentication authentication) {
        if (authentication != null) {
            printSectionHeader("Authentication configuration");
            // configuring mTLS (client TLS authentication, together with server authentication already set)
            if (authentication instanceof KafkaClientAuthenticationTls tlsAuth && tlsAuth.getCertificateAndKey() != null) {
                writer.println("kafka.ssl.keystore.location=/tmp/strimzi/bridge.keystore.p12");
                writer.println("kafka.ssl.keystore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
                writer.println("kafka.ssl.keystore.type=PKCS12");
            // otherwise SASL or OAuth is going to be used for authentication
            } else {
                securityProtocol = securityProtocol.equals("SSL") ? "SASL_SSL" : "SASL_PLAINTEXT";
                String saslMechanism = null;
                StringBuilder jaasConfig = new StringBuilder();

                if (authentication instanceof KafkaClientAuthenticationPlain passwordAuth) {
                    saslMechanism = "PLAIN";
                    String passwordFilePath = String.format(PLACEHOLDER_SASL_PASSWORD_FILE_TEMPLATE_CONFIG_PROVIDER_DIR, PASSWORD_VOLUME_MOUNT, passwordAuth.getPasswordSecret().getSecretName(), passwordAuth.getPasswordSecret().getPassword());
                    jaasConfig.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=" + PLACEHOLDER_SASL_USERNAME_CONFIG_PROVIDER_ENV_VAR + " password=" + passwordFilePath + ";");
                } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {

                    if (scramAuth.getType().equals(KafkaClientAuthenticationScramSha256.TYPE_SCRAM_SHA_256)) {
                        saslMechanism = "SCRAM-SHA-256";
                    } else if (scramAuth.getType().equals(KafkaClientAuthenticationScramSha512.TYPE_SCRAM_SHA_512)) {
                        saslMechanism = "SCRAM-SHA-512";
                    }

                    String passwordFilePath = String.format(PLACEHOLDER_SASL_PASSWORD_FILE_TEMPLATE_CONFIG_PROVIDER_DIR, PASSWORD_VOLUME_MOUNT, scramAuth.getPasswordSecret().getSecretName(), scramAuth.getPasswordSecret().getPassword());
                    jaasConfig.append("org.apache.kafka.common.security.scram.ScramLoginModule required username=" + PLACEHOLDER_SASL_USERNAME_CONFIG_PROVIDER_ENV_VAR + " password=" + passwordFilePath + ";");
                } else if (authentication instanceof KafkaClientAuthenticationOAuth oauth) {
                    saslMechanism = "OAUTHBEARER";
                    jaasConfig.append("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " + PLACEHOLDER_OAUTH_CONFIG_CONFIG_PROVIDER_ENV_VAR);

                    if (oauth.getClientSecret() != null) {
                        jaasConfig.append(" oauth.client.secret=" + PLACEHOLDER_OAUTH_CLIENT_SECRET_CONFIG_PROVIDER_ENV_VAR);
                    }

                    if (oauth.getRefreshToken() != null) {
                        jaasConfig.append(" oauth.refresh.token=" + PLACEHOLDER_OAUTH_REFRESH_TOKEN_CONFIG_PROVIDER_ENV_VAR);
                    }

                    if (oauth.getAccessToken() != null) {
                        jaasConfig.append(" oauth.access.token=" + PLACEHOLDER_OAUTH_ACCESS_TOKEN_CONFIG_PROVIDER_ENV_VAR);
                    }

                    if (oauth.getPasswordSecret() != null) {
                        jaasConfig.append(" oauth.password.grant.password=" + PLACEHOLDER_OAUTH_PASSWORD_GRANT_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
                    }

                    if (oauth.getTlsTrustedCertificates() != null && !oauth.getTlsTrustedCertificates().isEmpty()) {
                        jaasConfig.append(" oauth.ssl.truststore.location=\"/tmp/strimzi/oauth.truststore.p12\" oauth.ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR + " oauth.ssl.truststore.type=\"PKCS12\"");
                    }

                    jaasConfig.append(";");
                    writer.println("kafka.sasl.login.callback.handler.class=io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
                }
                writer.println("kafka.sasl.mechanism=" + saslMechanism);
                writer.println("kafka.sasl.jaas.config=" + jaasConfig);
                writer.println();
            }
        }
        return this;
    }

    /**
     * Adds the bridge Kafka admin client specific configuration
     *
     * @param kafkaBridgeAdminClient   the Kafka admin client configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withKafkaAdminClient(KafkaBridgeAdminClientSpec kafkaBridgeAdminClient) {
        if (kafkaBridgeAdminClient != null) {
            KafkaBridgeAdminClientConfiguration config = new KafkaBridgeAdminClientConfiguration(reconciliation, kafkaBridgeAdminClient.getConfig().entrySet());
            printSectionHeader("Apache Kafka AdminClient");
            config.asOrderedProperties().asMap().forEach((key, value) -> writer.println("kafka.admin." + key + "=" + value));
            writer.println();
        }
        return this;
    }

    /**
     * Adds the bridge Kafka producer specific configuration
     *
     * @param kafkaBridgeProducer   the Kafka producer configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withKafkaProducer(KafkaBridgeProducerSpec kafkaBridgeProducer) {
        if (kafkaBridgeProducer != null) {
            KafkaBridgeProducerConfiguration config = new KafkaBridgeProducerConfiguration(reconciliation, kafkaBridgeProducer.getConfig().entrySet());
            printSectionHeader("Apache Kafka Producer");
            config.asOrderedProperties().asMap().forEach((key, value) -> writer.println("kafka.producer." + key + "=" + value));
            writer.println();
        }
        return this;
    }

    /**
     * Adds the bridge Kafka consumer specific configuration
     *
     * @param kafkaBridgeConsumer   the Kafka consumer configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withKafkaConsumer(KafkaBridgeConsumerSpec kafkaBridgeConsumer) {
        if (kafkaBridgeConsumer != null) {
            KafkaBridgeConsumerConfiguration config = new KafkaBridgeConsumerConfiguration(reconciliation, kafkaBridgeConsumer.getConfig().entrySet());
            printSectionHeader("Apache Kafka Consumer");
            config.asOrderedProperties().asMap().forEach((key, value) -> writer.println("kafka.consumer." + key + "=" + value));
            writer.println("kafka.consumer.client.rack=${strimzidir:/opt/strimzi/init:rack.id}");
            writer.println();
        }
        return this;
    }

    /**
     * Adds the HTTP configuration which includes HTTP specific parameters (i.e. host, port, CORS, ...) as well as
     * configuration for the HTTP related part of the producer and consumer (i.e. timeout, enable status, ...)
     *
     * @param http  the HTTP configuration
     * @param kafkaBridgeProducer   the Kafka producer configuration
     * @param kafkaBridgeConsumer   the Kafka consumer configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withHttp(KafkaBridgeHttpConfig http, KafkaBridgeProducerSpec kafkaBridgeProducer, KafkaBridgeConsumerSpec kafkaBridgeConsumer) {
        printSectionHeader("HTTP configuration");
        writer.println("http.host=" + KafkaBridgeHttpConfig.HTTP_DEFAULT_HOST);
        writer.println("http.port=" + (http != null ? http.getPort() : KafkaBridgeHttpConfig.HTTP_DEFAULT_PORT));
        if (http != null && http.getCors() != null) {
            writer.println("http.cors.enabled=true");

            if (http.getCors().getAllowedOrigins() != null) {
                writer.println("http.cors.allowedOrigins=" + String.join(",", http.getCors().getAllowedOrigins()));
            }

            if (http.getCors().getAllowedMethods() != null) {
                writer.println("http.cors.allowedMethods=" + String.join(",", http.getCors().getAllowedMethods()));
            }
        } else {
            writer.println("http.cors.enabled=false");
        }

        if (kafkaBridgeConsumer != null) {
            writer.println("http.consumer.enabled=" + kafkaBridgeConsumer.isEnabled());
            writer.println("http.timeoutSeconds=" + kafkaBridgeConsumer.getTimeoutSeconds());
        } else {
            writer.println("http.consumer.enabled=true");
            writer.println("http.timeoutSeconds=" + KafkaBridgeConsumerSpec.HTTP_DEFAULT_TIMEOUT);
        }

        if (kafkaBridgeProducer != null) {
            writer.println("http.producer.enabled=" + kafkaBridgeProducer.isEnabled());
        } else {
            writer.println("http.producer.enabled=true");
        }

        return this;
    }

    /**
     * Prints the file header which is on the beginning of the configuration file.
     */
    private void printHeader()   {
        writer.println("##############################");
        writer.println("##############################");
        writer.println("# This file is automatically generated by the Strimzi Cluster Operator");
        writer.println("# Any changes to this file will be ignored and overwritten!");
        writer.println("##############################");
        writer.println("##############################");
        writer.println();
    }

    /**
     * Internal method which prints the section header into the configuration file. This makes it more human-readable
     * when looking for issues in running pods etc.
     *
     * @param sectionName   Name of the section for which is this header printed
     */
    private void printSectionHeader(String sectionName)   {
        writer.println("##########");
        writer.println("# " + sectionName);
        writer.println("##########");
    }

    /**
     * Generates the configuration template as String
     *
     * @return String with the Kafka bridge configuration template
     */
    public String build()  {
        configureSecurityProtocol();
        return stringWriter.toString();
    }
}