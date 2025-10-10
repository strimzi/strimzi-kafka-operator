/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationCustom;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.tracing.JaegerTracing;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ConnectorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2TargetClusterSpec;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterConfig;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster.MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT;
import static io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster.MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT;
import static io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT;

/**
 * Kafka Mirror Maker 2 Connectors model
 */
public class KafkaMirrorMaker2Connectors {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaMirrorMaker2Connectors.class.getName());

    private static final String CONNECTOR_JAVA_PACKAGE = "org.apache.kafka.connect.mirror";
    private static final String TARGET_CLUSTER_PREFIX = "target.cluster.";
    private static final String SOURCE_CLUSTER_PREFIX = "source.cluster.";
    private static final String CONNECT_CONFIG_FILE = "/tmp/strimzi-connect.properties";
    private static final String SOURCE_CONNECTOR_SUFFIX = ".MirrorSourceConnector";
    private static final String CHECKPOINT_CONNECTOR_SUFFIX = ".MirrorCheckpointConnector";
    private static final String HEARTBEAT_CONNECTOR_SUFFIX = ".MirrorHeartbeatConnector";
    // the secrets file template: <namespace>/<secret_name>:<secret_key>
    private static final String PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER = "${strimzisecrets:%s/%s:%s}";

    private static final String PLACEHOLDER_MIRRORMAKER2_CONNECTOR_CONFIGS_TEMPLATE_CONFIG_PROVIDER_DIR = "${strimzidir:%s%s/%s:%s}";

    @SuppressWarnings("deprecation") // Heartbeat connector is deprecated
    private static final Map<String, Function<KafkaMirrorMaker2MirrorSpec, KafkaMirrorMaker2ConnectorSpec>> CONNECTORS = Map.of(
            SOURCE_CONNECTOR_SUFFIX, KafkaMirrorMaker2MirrorSpec::getSourceConnector,
            CHECKPOINT_CONNECTOR_SUFFIX, KafkaMirrorMaker2MirrorSpec::getCheckpointConnector,
            HEARTBEAT_CONNECTOR_SUFFIX, KafkaMirrorMaker2MirrorSpec::getHeartbeatConnector
    );

    private final Reconciliation reconciliation;
    private final KafkaMirrorMaker2TargetClusterSpec target;
    private final List<KafkaMirrorMaker2MirrorSpec> mirrors;
    private final Tracing tracing;
    private final boolean rackAwarenessEnabled;
    private final boolean strimziMetricsReporterEnabled;

    /**
     * Constructor
     *
     * @param reconciliation        Reconciliation marker
     * @param kafkaMirrorMaker2     KafkaMirrorMaker2 custom resource
     */
    private KafkaMirrorMaker2Connectors(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2) {
        this.reconciliation = reconciliation;
        this.target = kafkaMirrorMaker2.getSpec().getTarget();
        this.mirrors = kafkaMirrorMaker2.getSpec().getMirrors();
        this.tracing = kafkaMirrorMaker2.getSpec().getTracing();
        this.rackAwarenessEnabled = kafkaMirrorMaker2.getSpec().getRack() != null;
        this.strimziMetricsReporterEnabled = kafkaMirrorMaker2.getSpec().getMetricsConfig() instanceof StrimziMetricsReporter;
    }

    /**
     * Creates and returns a Mirror Maker 2 Connectors instance
     *
     * @param reconciliation        Reconciliation marker
     * @param kafkaMirrorMaker2     KafkaMirrorMaker2 custom resource
     *
     * @return  Newly created KafkaMirrorMaker2Connectors instance
     */
    public static KafkaMirrorMaker2Connectors fromCrd(Reconciliation reconciliation, KafkaMirrorMaker2 kafkaMirrorMaker2)    {
        return new KafkaMirrorMaker2Connectors(reconciliation, kafkaMirrorMaker2);
    }

    /**
     * @return  List with connector definitions for this Mirror Maker 2 cluster
     */
    public List<KafkaConnector> generateConnectorDefinitions()    {
        List<KafkaConnector> connectors = new ArrayList<>();

        for (KafkaMirrorMaker2MirrorSpec mirror : mirrors)    {
            for (Entry<String, Function<KafkaMirrorMaker2MirrorSpec, KafkaMirrorMaker2ConnectorSpec>> connectorType : CONNECTORS.entrySet())   {
                // Get the connector spec from the MM2 CR definitions
                KafkaMirrorMaker2ConnectorSpec mm2ConnectorSpec = connectorType.getValue().apply(mirror);

                if (mm2ConnectorSpec != null) {
                    @SuppressWarnings("deprecation") // getPause() is deprecated
                    KafkaConnector connector = new KafkaConnectorBuilder()
                            .withNewMetadata()
                                .withName(mirror.getSource().getAlias() + "->" + target.getAlias() + connectorType.getKey())
                            .endMetadata()
                            .withNewSpec()
                                .withClassName(CONNECTOR_JAVA_PACKAGE + connectorType.getKey())
                                .withConfig(prepareMirrorMaker2ConnectorConfig(mirror, mm2ConnectorSpec))
                                .withPause(mm2ConnectorSpec.getPause())
                                .withState(mm2ConnectorSpec.getState())
                                .withAutoRestart(mm2ConnectorSpec.getAutoRestart())
                                .withTasksMax(mm2ConnectorSpec.getTasksMax())
                                .withListOffsets(mm2ConnectorSpec.getListOffsets())
                                .withAlterOffsets(mm2ConnectorSpec.getAlterOffsets())
                            .endSpec()
                            .build();

                    connectors.add(connector);
                }
            }
        }

        return connectors;
    }

    @SuppressWarnings("NPathComplexity")
    /* test */ Map<String, Object> prepareMirrorMaker2ConnectorConfig(KafkaMirrorMaker2MirrorSpec mirror, KafkaMirrorMaker2ConnectorSpec connector) {
        Map<String, Object> config = new HashMap<>();

        // Source and target cluster configurations
        addClusterToMirrorMaker2ConnectorConfig(reconciliation, config, target, TARGET_CLUSTER_PREFIX);
        addClusterToMirrorMaker2ConnectorConfig(reconciliation, config, mirror.getSource(), SOURCE_CLUSTER_PREFIX);

        // Add connector config to the configuration
        config.putAll(connector.getConfig());

        // Topics pattern
        if (mirror.getTopicsPattern() != null) {
            config.put("topics", mirror.getTopicsPattern());
        }

        // Topics exclusion pattern
        String topicsExcludePattern = mirror.getTopicsExcludePattern();
        @SuppressWarnings("deprecation") // getTopicsBlacklistPattern() is deprecated
        String topicsBlacklistPattern = mirror.getTopicsBlacklistPattern();
        if (topicsExcludePattern != null && topicsBlacklistPattern != null) {
            LOGGER.warnCr(reconciliation, "Both topicsExcludePattern and topicsBlacklistPattern mirror properties are present, ignoring topicsBlacklistPattern as it is deprecated");
        }
        String topicsExclude = topicsExcludePattern != null ? topicsExcludePattern : topicsBlacklistPattern;
        if (topicsExclude != null) {
            config.put("topics.exclude", topicsExclude);
        }

        // Groups pattern
        if (mirror.getGroupsPattern() != null) {
            config.put("groups", mirror.getGroupsPattern());
        }

        // Groups exclusion pattern
        String groupsExcludePattern = mirror.getGroupsExcludePattern();
        @SuppressWarnings("deprecation") // getGroupsBlacklistPattern() is deprecated
        String groupsBlacklistPattern = mirror.getGroupsBlacklistPattern();
        if (groupsExcludePattern != null && groupsBlacklistPattern != null) {
            LOGGER.warnCr(reconciliation, "Both groupsExcludePattern and groupsBlacklistPattern mirror properties are present, ignoring groupsBlacklistPattern as it is deprecated");
        }
        String groupsExclude = groupsExcludePattern != null ? groupsExcludePattern :  groupsBlacklistPattern;
        if (groupsExclude != null) {
            config.put("groups.exclude", groupsExclude);
        }

        // Tracing
        if (tracing != null)   {
            @SuppressWarnings("deprecation") // JaegerTracing is deprecated
            String jaegerType = JaegerTracing.TYPE_JAEGER;

            if (jaegerType.equals(tracing.getType())) {
                LOGGER.warnCr(reconciliation, "Tracing type \"{}\" is not supported anymore and will be ignored", jaegerType);
            } else if (OpenTelemetryTracing.TYPE_OPENTELEMETRY.equals(tracing.getType())) {
                config.put("consumer.interceptor.classes", OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME);
                config.put("producer.interceptor.classes", OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME);
            }
        }

        // Rack awareness (client.rack has to be configured in the connector because the consumer is created by the connector)
        if (rackAwarenessEnabled) {
            String clientRackKey = "consumer.client.rack";
            config.put(clientRackKey, "${strimzifile:" + CONNECT_CONFIG_FILE + ":" + clientRackKey + "}");
        }

        if (strimziMetricsReporterEnabled) {
            // MM2 connectors metrics are collected through this dedicated SMR instance
            // into a shared Prometheus registry instance, so they can be exposed through
            // the Kafka Connect SMR listener endpoint without enabling a new listener
            Object existingValue = config.get("metric.reporters");
            String newValue = StrimziMetricsReporterConfig.KAFKA_CLASS;
            config.put("metric.reporters", existingValue != null
                    && !existingValue.toString().contains(StrimziMetricsReporterConfig.KAFKA_CLASS)
                        ? existingValue + "," + newValue : newValue);
            config.put(StrimziMetricsReporterConfig.LISTENER_ENABLE, "false");
        }

        return config;
    }

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    /* test */ static void addClusterToMirrorMaker2ConnectorConfig(Reconciliation reconciliation, Map<String, Object> config, KafkaMirrorMaker2ClusterSpec cluster, String configPrefix) {
        config.put(configPrefix + "alias", cluster.getAlias());
        config.put(configPrefix + AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());

        String securityProtocol = addTLSConfigToMirrorMaker2ConnectorConfig(config, cluster, configPrefix, clusterName, namespace);

        if (cluster.getAuthentication() != null) {
            if (cluster.getAuthentication() instanceof KafkaClientAuthenticationTls tlsAuth) {
                config.put(configPrefix + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
                String certConfigProviderValue = String.format(PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER, namespace, tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getCertificate());
                String keyConfigProviderValue = String.format(PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER, namespace, tlsAuth.getCertificateAndKey().getSecretName(), tlsAuth.getCertificateAndKey().getKey());
                config.put(configPrefix + SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, certConfigProviderValue);
                config.put(configPrefix + SslConfigs.SSL_KEYSTORE_KEY_CONFIG, keyConfigProviderValue);
            } else if (cluster.getAuthentication() instanceof KafkaClientAuthenticationPlain plainAuthentication) {
                securityProtocol = cluster.getTls() != null ? "SASL_SSL" : "SASL_PLAINTEXT";
                config.put(configPrefix + SaslConfigs.SASL_MECHANISM, "PLAIN");
                String passwordFilePath = String.format(PLACEHOLDER_MIRRORMAKER2_CONNECTOR_CONFIGS_TEMPLATE_CONFIG_PROVIDER_DIR, MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT, cluster.getAlias(), plainAuthentication.getPasswordSecret().getSecretName(), plainAuthentication.getPasswordSecret().getPassword());
                config.put(configPrefix + SaslConfigs.SASL_JAAS_CONFIG,
                        AuthenticationUtils.jaasConfig("org.apache.kafka.common.security.plain.PlainLoginModule",
                                Map.of("username", plainAuthentication.getUsername(),
                                        "password", passwordFilePath)));
            } else if (cluster.getAuthentication() instanceof KafkaClientAuthenticationScram scramAuthentication) {
                securityProtocol = cluster.getTls() != null ? "SASL_SSL" : "SASL_PLAINTEXT";
                config.put(configPrefix + SaslConfigs.SASL_MECHANISM, scramAuthentication instanceof KafkaClientAuthenticationScramSha256 ? "SCRAM-SHA-256" : "SCRAM-SHA-512");
                String passwordFilePath = String.format(PLACEHOLDER_MIRRORMAKER2_CONNECTOR_CONFIGS_TEMPLATE_CONFIG_PROVIDER_DIR, MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT, cluster.getAlias(), scramAuthentication.getPasswordSecret().getSecretName(), scramAuthentication.getPasswordSecret().getPassword());
                config.put(configPrefix + SaslConfigs.SASL_JAAS_CONFIG,
                        AuthenticationUtils.jaasConfig("org.apache.kafka.common.security.scram.ScramLoginModule",
                                Map.of("username", scramAuthentication.getUsername(),
                                        "password", passwordFilePath)));
            } else if (cluster.getAuthentication() instanceof KafkaClientAuthenticationOAuth oauthAuthentication) {
                securityProtocol = cluster.getTls() != null ? "SASL_SSL" : "SASL_PLAINTEXT";
                config.put(configPrefix + SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
                config.put(configPrefix + SaslConfigs.SASL_JAAS_CONFIG,
                        oauthJaasConfig(cluster, oauthAuthentication, clusterName));
                config.put(configPrefix + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
            } else if (cluster.getAuthentication() instanceof KafkaClientAuthenticationCustom customAuth) { // Configure custom authentication
                if (customAuth.isSasl()) {
                    // If this authentication uses SASL, we need to update the security protocol to combine the SASL
                    // flag with the SSL or PLAINTEXT flag.
                    securityProtocol = securityProtocol.equals("SSL") ? "SASL_SSL" : "SASL_PLAINTEXT";
                }

                Map<String, Object> customConfig = customAuth.getConfig();
                if (customConfig == null) {
                    customConfig = Map.of();
                }

                KafkaClientAuthenticationCustomConfiguration authConfig = new KafkaClientAuthenticationCustomConfiguration(reconciliation, customConfig.entrySet());
                authConfig.asOrderedProperties().asMap().forEach((key, value) -> config.put(configPrefix + key, value));
            }
        }

        // Security protocol
        config.put(configPrefix + AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        config.putAll(cluster.getConfig().entrySet().stream()
                .collect(Collectors.toMap(entry -> configPrefix + entry.getKey(), Map.Entry::getValue)));
        config.putAll(cluster.getAdditionalProperties());
    }

    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    private static String oauthJaasConfig(KafkaMirrorMaker2ClusterSpec cluster, KafkaClientAuthenticationOAuth oauth) {
        Map<String, String> jaasOptions = cluster.getAuthentication() instanceof KafkaClientAuthenticationOAuth ? AuthenticationUtils.oauthJaasOptions((KafkaClientAuthenticationOAuth) cluster.getAuthentication()) : new LinkedHashMap<>();

        if (oauth.getClientSecret() != null) {
            jaasOptions.put("oauth.client.secret", String.format(PLACEHOLDER_MIRRORMAKER2_CONNECTOR_CONFIGS_TEMPLATE_CONFIG_PROVIDER_DIR, MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT, cluster.getAlias(), oauth.getClientSecret().getSecretName(), oauth.getClientSecret().getKey()));
        }

        if (oauth.getAccessToken() != null) {
            jaasOptions.put("oauth.access.token", String.format(PLACEHOLDER_MIRRORMAKER2_CONNECTOR_CONFIGS_TEMPLATE_CONFIG_PROVIDER_DIR, MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT, cluster.getAlias(), oauth.getAccessToken().getSecretName(), oauth.getAccessToken().getKey()));
        }

        if (oauth.getRefreshToken() != null) {
            jaasOptions.put("oauth.refresh.token", String.format(PLACEHOLDER_MIRRORMAKER2_CONNECTOR_CONFIGS_TEMPLATE_CONFIG_PROVIDER_DIR, MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT, cluster.getAlias(), oauth.getRefreshToken().getSecretName(), oauth.getRefreshToken().getKey()));
        }

        if (oauth.getPasswordSecret() != null) {
            jaasOptions.put("oauth.password.grant.password", String.format(PLACEHOLDER_MIRRORMAKER2_CONNECTOR_CONFIGS_TEMPLATE_CONFIG_PROVIDER_DIR, MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT, cluster.getAlias(), oauth.getPasswordSecret().getSecretName(), oauth.getPasswordSecret().getPassword()));
        }

        if (oauth.getClientAssertion() != null) {
            jaasOptions.put("oauth.client.assertion", String.format(PLACEHOLDER_MIRRORMAKER2_CONNECTOR_CONFIGS_TEMPLATE_CONFIG_PROVIDER_DIR, MIRRORMAKER_2_OAUTH_SECRETS_BASE_VOLUME_MOUNT, cluster.getAlias(), oauth.getClientAssertion().getSecretName(), oauth.getClientAssertion().getKey()));
        }

        if (oauth.getTlsTrustedCertificates() != null && !oauth.getTlsTrustedCertificates().isEmpty()) {
            String oauthTrustedCertsSecret = KafkaConnectResources.internalOauthTrustedCertsSecretName(clusterName);
            jaasOptions.put("oauth." + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, MIRRORMAKER_2_OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT + oauthTrustedCertsSecret + "/" + oauthTrustedCertsSecret + ".crt");
            jaasOptions.put("oauth." + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        }

        return AuthenticationUtils.jaasConfig("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule", jaasOptions);
    }

    /**
     * Adds configuration for the TLS encryption to the map if TLS is configured and returns the security protocol
     * (SSL or PLAINTEXT). The returned security protocol is later modified for SASL if needed, but this is done in the
     * parent method.
     *
     * @param config        Map with the configuration
     * @param cluster       Cluster configuration (.spec.clusters property from the MM2 custom resource)
     * @param configPrefix  Prefix string for the added configuration options
     *
     * @return  String indicating whether the security protocol should be SSL or PLAINTEXT based
     */
    private static String addTLSConfigToMirrorMaker2ConnectorConfig(Map<String, Object> config, KafkaMirrorMaker2ClusterSpec cluster, String configPrefix, String clusterName, String namespace) {
        if (cluster.getTls() != null) {
            if (cluster.getTls().getTrustedCertificates() != null && !cluster.getTls().getTrustedCertificates().isEmpty()) {
                config.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
                String configProviderValue = String.format(PLACEHOLDER_SECRET_TEMPLATE_KUBE_CONFIG_PROVIDER, namespace, KafkaConnectResources.internalTlsTrustedCertsSecretName(clusterName), "*.crt");
                config.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, configProviderValue);
            }

            return "SSL";
        } else {
            return "PLAINTEXT";
        }
    }
}
