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
import io.strimzi.api.kafka.model.common.GenericSecretSource;
import io.strimzi.api.kafka.model.common.PasswordSecretSource;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha512;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.common.metrics.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.operator.cluster.model.metrics.JmxPrometheusExporterModel;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterConfig;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterModel;
import io.strimzi.operator.common.Reconciliation;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.stream.Collectors;

import static io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter.TYPE_STRIMZI_METRICS_REPORTER;
import static io.strimzi.operator.cluster.model.KafkaBridgeCluster.KAFKA_BRIDGE_CONFIG_VOLUME_MOUNT;
import static io.strimzi.operator.cluster.model.KafkaBridgeCluster.OAUTH_SECRETS_BASE_VOLUME_MOUNT;

/**
 * This class is used to generate the bridge configuration template. The template is later passed using a ConfigMap to
 * the bridge pod. The script in the container image will fill in the variables in the template and use the
 * configuration file. This class is using the builder pattern to make it easy to test the different parts etc. To
 * generate the configuration file, it is using the PrintWriter.
 */
public class KafkaBridgeConfigurationBuilder {

    // placeholders expanded through config providers inside the bridge node
    private static final String PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:CERTS_STORE_PASSWORD}";
    private static final String PASSWORD_VOLUME_MOUNT = "/opt/strimzi/bridge-password/";
    // the volume mounted secret file template includes: <volume_mount>/<secret_name>/<secret_key>
    private static final String PLACEHOLDER_VOLUME_MOUNTED_SECRET_TEMPLATE_CONFIG_PROVIDER_DIR = "${strimzidir:%s%s:%s}";

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
                    jaasConfig.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + passwordAuth.getUsername() + "\" password=\"" + formatPasswordTemplate(passwordAuth.getPasswordSecret(), PASSWORD_VOLUME_MOUNT) + "\";");
                } else if (authentication instanceof KafkaClientAuthenticationScram scramAuth) {

                    if (scramAuth.getType().equals(KafkaClientAuthenticationScramSha256.TYPE_SCRAM_SHA_256)) {
                        saslMechanism = "SCRAM-SHA-256";
                    } else if (scramAuth.getType().equals(KafkaClientAuthenticationScramSha512.TYPE_SCRAM_SHA_512)) {
                        saslMechanism = "SCRAM-SHA-512";
                    }

                    jaasConfig.append("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + scramAuth.getUsername() + "\" password=\"" + formatPasswordTemplate(scramAuth.getPasswordSecret(), PASSWORD_VOLUME_MOUNT) + "\";");
                } else if (authentication instanceof KafkaClientAuthenticationOAuth oauth) {
                    saslMechanism = "OAUTHBEARER";
                    String oauthConfig = AuthenticationUtils.oauthJaasOptions(oauth).entrySet().stream()
                            .map(e -> e.getKey() + "=\"" + e.getValue() + "\"")
                            .collect(Collectors.joining(" "));

                    if (!oauthConfig.isEmpty()) {
                        jaasConfig.append("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " + oauthConfig);
                    } else {
                        jaasConfig.append("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required");
                    }

                    if (oauth.getClientSecret() != null) {
                        jaasConfig.append(" oauth.client.secret=\"" + formatOauthSecretTemplate(oauth.getClientSecret()) + "\"");
                    }

                    if (oauth.getRefreshToken() != null) {
                        jaasConfig.append(" oauth.refresh.token=\"" + formatOauthSecretTemplate(oauth.getRefreshToken()) + "\"");
                    }

                    if (oauth.getAccessToken() != null) {
                        jaasConfig.append(" oauth.access.token=\"" + formatOauthSecretTemplate(oauth.getAccessToken()) + "\"");
                    }

                    if (oauth.getPasswordSecret() != null) {
                        jaasConfig.append(" oauth.password.grant.password=\"" + formatPasswordTemplate(oauth.getPasswordSecret(), OAUTH_SECRETS_BASE_VOLUME_MOUNT) + "\"");
                    }

                    if (oauth.getClientAssertion() != null) {
                        jaasConfig.append(" oauth.client.assertion=\"" + formatOauthSecretTemplate(oauth.getClientAssertion()) + "\"");
                    }

                    if (oauth.getTlsTrustedCertificates() != null && !oauth.getTlsTrustedCertificates().isEmpty()) {
                        jaasConfig.append(" oauth.ssl.truststore.location=\"/tmp/strimzi/oauth.truststore.p12\" oauth.ssl.truststore.password=\"" + PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR + "\" oauth.ssl.truststore.type=\"PKCS12\"");
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

    private String formatOauthSecretTemplate(GenericSecretSource secret) {
        return String.format(PLACEHOLDER_VOLUME_MOUNTED_SECRET_TEMPLATE_CONFIG_PROVIDER_DIR, OAUTH_SECRETS_BASE_VOLUME_MOUNT, secret.getSecretName(), secret.getKey());
    }

    private String formatPasswordTemplate(PasswordSecretSource secret, String volumeMountPath) {
        return String.format(PLACEHOLDER_VOLUME_MOUNTED_SECRET_TEMPLATE_CONFIG_PROVIDER_DIR, volumeMountPath, secret.getSecretName(), secret.getPassword());
    }

    /**
     * Configures the Kafka configuration providers
     *
     * @param userConfig    the user configuration, for a specific bridge Kafka client (admin, producer or consumer)
     *                      to extract the possible user-provided config provider configuration from it
     * @param prefix    prefix for the bridge Kafka client to be configured. It could be "kafka.admin", "kafka.producer" or "kafka.consumer".
     */
    private void configProvider(AbstractConfiguration userConfig, String prefix) {
        printSectionHeader("Config providers");
        String strimziConfigProviders = "strimzienv,strimzifile,strimzidir";
        // configure user provided config providers together with the Strimzi ones ...
        if (userConfig != null
                && !userConfig.getConfiguration().isEmpty()
                && userConfig.getConfigOption("config.providers") != null) {
            writer.println(prefix + ".config.providers=" + userConfig.getConfigOption("config.providers") + "," + strimziConfigProviders);
            userConfig.removeConfigOption("config.providers");
        // ... or configure only the Strimzi config providers
        } else {
            writer.println(prefix + ".config.providers=" + strimziConfigProviders);
        }
        writer.println(prefix + ".config.providers.strimzienv.class=org.apache.kafka.common.config.provider.EnvVarConfigProvider");
        writer.println(prefix + ".config.providers.strimzienv.param.allowlist.pattern=.*");
        writer.println(prefix + ".config.providers.strimzifile.class=org.apache.kafka.common.config.provider.FileConfigProvider");
        writer.println(prefix + ".config.providers.strimzifile.param.allowed.paths=/opt/strimzi");
        writer.println(prefix + ".config.providers.strimzidir.class=org.apache.kafka.common.config.provider.DirectoryConfigProvider");
        writer.println(prefix + ".config.providers.strimzidir.param.allowed.paths=/opt/strimzi");
    }

    /**
     * Adds the bridge Kafka admin client specific configuration
     *
     * @param kafkaBridgeAdminClient   the Kafka admin client configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withKafkaAdminClient(KafkaBridgeAdminClientSpec kafkaBridgeAdminClient) {
        printSectionHeader("Apache Kafka AdminClient");
        KafkaBridgeAdminClientConfiguration config = kafkaBridgeAdminClient != null ?
                new KafkaBridgeAdminClientConfiguration(reconciliation, kafkaBridgeAdminClient.getConfig().entrySet()) :
                null;
        configProvider(config, "kafka.admin");
        if (config != null) {
            config.asOrderedProperties().asMap().forEach((key, value) -> writer.println("kafka.admin." + key + "=" + value));
        }
        writer.println();
        return this;
    }

    /**
     * Adds the bridge Kafka producer specific configuration
     *
     * @param kafkaBridgeProducer   the Kafka producer configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withKafkaProducer(KafkaBridgeProducerSpec kafkaBridgeProducer) {
        printSectionHeader("Apache Kafka Producer");
        KafkaBridgeProducerConfiguration config = kafkaBridgeProducer != null ?
                new KafkaBridgeProducerConfiguration(reconciliation, kafkaBridgeProducer.getConfig().entrySet()) :
                null;
        configProvider(config, "kafka.producer");
        if (config != null) {
            config.asOrderedProperties().asMap().forEach((key, value) -> writer.println("kafka.producer." + key + "=" + value));
        }
        writer.println();
        return this;
    }

    /**
     * Adds the bridge Kafka consumer specific configuration
     *
     * @param kafkaBridgeConsumer   the Kafka consumer configuration
     * @return  the builder instance
     */
    public KafkaBridgeConfigurationBuilder withKafkaConsumer(KafkaBridgeConsumerSpec kafkaBridgeConsumer) {
        printSectionHeader("Apache Kafka Consumer");
        KafkaBridgeConsumerConfiguration config = kafkaBridgeConsumer != null ?
                new KafkaBridgeConsumerConfiguration(reconciliation, kafkaBridgeConsumer.getConfig().entrySet()) :
                null;
        configProvider(config, "kafka.consumer");
        if (config != null) {
            config.asOrderedProperties().asMap().forEach((key, value) -> writer.println("kafka.consumer." + key + "=" + value));
            writer.println("kafka.consumer.client.rack=${strimzidir:/opt/strimzi/init:rack.id}");
        }
        writer.println();
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
     * Configures the Strimzi Metrics Reporter. It is set only if user enables Strimzi Metrics Reporter.
     *
     * @param model     Strimzi Metrics Reporter configuration
     *
     * @return Returns the builder instance
     */
    public KafkaBridgeConfigurationBuilder withStrimziMetricsReporter(StrimziMetricsReporterModel model)   {
        if (model != null) {
            printSectionHeader("Strimzi Metrics Reporter configuration");
            writer.println("bridge.metrics=" + TYPE_STRIMZI_METRICS_REPORTER);
            // the kafka. prefix is required by the Bridge to pass Kafka client configurations
            writer.println("kafka.metric.reporters=" + StrimziMetricsReporterConfig.KAFKA_CLASS);
            writer.println("kafka." + StrimziMetricsReporterConfig.LISTENER_ENABLE + "=false");
            writer.println("kafka." + StrimziMetricsReporterConfig.ALLOW_LIST + "=" + model.getAllowList());
            writer.println();
        }
        return this;
    }

    /**
     * Configures the JMX Prometheus Metrics Exporter.
     *
     * @param model JMX Prometheus Metrics Exporter configuration
     * @param isLegacyMetricsConfigEnabled Flag which indicates whether the metrics are enabled or not in legacy mode.
     *
     * @return Returns the builder instance
     */
    public KafkaBridgeConfigurationBuilder withJmxPrometheusExporter(
            JmxPrometheusExporterModel model, boolean isLegacyMetricsConfigEnabled) {
        if (model != null || isLegacyMetricsConfigEnabled) {
            printSectionHeader("Prometheus JMX Exporter configuration");
            writer.println("bridge.metrics=" + JmxPrometheusExporterMetrics.TYPE_JMX_EXPORTER);

            // if isLegacyMetricsConfigEnabled is not used, we pass the path of the config file.
            // If it is used, the Bridge will use the fallback config.
            if (!isLegacyMetricsConfigEnabled) {
                writer.println("bridge.metrics.exporter.config.path="
                        + KAFKA_BRIDGE_CONFIG_VOLUME_MOUNT + JmxPrometheusExporterModel.CONFIG_MAP_KEY);
            }

            writer.println();
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
