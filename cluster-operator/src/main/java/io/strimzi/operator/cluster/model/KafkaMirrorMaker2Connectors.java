/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationCustom;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationPlain;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScram;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationScramSha256;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationTls;
import io.strimzi.api.kafka.model.common.metrics.StrimziMetricsReporter;
import io.strimzi.api.kafka.model.common.tracing.OpenTelemetryTracing;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ConnectorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2TargetClusterSpec;
import io.strimzi.operator.cluster.model.metrics.StrimziMetricsReporterConfig;
import io.strimzi.operator.common.Reconciliation;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.KafkaMirrorMaker2Cluster.MIRRORMAKER_2_PASSWORD_VOLUME_MOUNT;

/**
 * Kafka Mirror Maker 2 Connectors model
 */
public class KafkaMirrorMaker2Connectors {
    private static final String CONNECTOR_JAVA_PACKAGE = "org.apache.kafka.connect.mirror";
    private static final String TARGET_CLUSTER_PREFIX = "target.cluster.";
    private static final String SOURCE_CLUSTER_PREFIX = "source.cluster.";
    private static final String STORE_LOCATION_ROOT = "/tmp/kafka/clusters/";
    private static final String TRUSTSTORE_SUFFIX = ".truststore.p12";
    private static final String KEYSTORE_SUFFIX = ".keystore.p12";
    private static final String CONNECT_CONFIG_FILE = "/tmp/strimzi-connect.properties";
    private static final String SOURCE_CONNECTOR_SUFFIX = ".MirrorSourceConnector";
    private static final String CHECKPOINT_CONNECTOR_SUFFIX = ".MirrorCheckpointConnector";
    protected static final String PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR = "${strimzienv:MIRRORMAKER_2_CERTS_STORE_PASSWORD}";

    private static final String PLACEHOLDER_MIRRORMAKER2_CONNECTOR_CONFIGS_TEMPLATE_CONFIG_PROVIDER_DIR = "${strimzidir:%s%s/%s:%s}";

    private static final Map<String, Function<KafkaMirrorMaker2MirrorSpec, KafkaMirrorMaker2ConnectorSpec>> CONNECTORS = Map.of(
            SOURCE_CONNECTOR_SUFFIX, KafkaMirrorMaker2MirrorSpec::getSourceConnector,
            CHECKPOINT_CONNECTOR_SUFFIX, KafkaMirrorMaker2MirrorSpec::getCheckpointConnector
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
     * Generates the list of connector definitions for this Mirror Maker 2 cluster.
     *
     * @return  List with connector definitions for this Mirror Maker 2 cluster
     */
    public List<KafkaConnector> generateConnectorDefinitions()    {
        List<KafkaConnector> connectors = new ArrayList<>();

        for (KafkaMirrorMaker2MirrorSpec mirror : mirrors)    {
            for (Entry<String, Function<KafkaMirrorMaker2MirrorSpec, KafkaMirrorMaker2ConnectorSpec>> connectorType : CONNECTORS.entrySet())   {
                // Get the connector spec from the MM2 CR definitions
                KafkaMirrorMaker2ConnectorSpec mm2ConnectorSpec = connectorType.getValue().apply(mirror);

                if (mm2ConnectorSpec != null) {
                    KafkaConnector connector = new KafkaConnectorBuilder()
                            .withNewMetadata()
                                .withName(mirror.getSource().getAlias() + "->" + target.getAlias() + connectorType.getKey())
                            .endMetadata()
                            .withNewSpec()
                                .withClassName(CONNECTOR_JAVA_PACKAGE + connectorType.getKey())
                                .withConfig(prepareMirrorMaker2ConnectorConfig(mirror, mm2ConnectorSpec))
                                .withState(mm2ConnectorSpec.getState())
                                .withAutoRestart(mm2ConnectorSpec.getAutoRestart())
                                .withTasksMax(mm2ConnectorSpec.getTasksMax())
                                .withVersion(mm2ConnectorSpec.getVersion())
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
        if (topicsExcludePattern != null) {
            config.put("topics.exclude", topicsExcludePattern);
        }

        // Groups pattern
        if (mirror.getGroupsPattern() != null) {
            config.put("groups", mirror.getGroupsPattern());
        }

        // Groups exclusion pattern
        String groupsExcludePattern = mirror.getGroupsExcludePattern();
        if (groupsExcludePattern != null) {
            config.put("groups.exclude", groupsExcludePattern);
        }

        // Tracing
        if (tracing != null
                && OpenTelemetryTracing.TYPE_OPENTELEMETRY.equals(tracing.getType()))   {
            config.put("consumer.interceptor.classes", OpenTelemetryTracing.CONSUMER_INTERCEPTOR_CLASS_NAME);
            config.put("producer.interceptor.classes", OpenTelemetryTracing.PRODUCER_INTERCEPTOR_CLASS_NAME);
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
            String newValue = StrimziMetricsReporterConfig.CLIENT_CLASS;
            config.put("metric.reporters", existingValue != null
                    && !existingValue.toString().contains(StrimziMetricsReporterConfig.CLIENT_CLASS)
                        ? existingValue + "," + newValue : newValue);
            config.put(StrimziMetricsReporterConfig.LISTENER_ENABLE, "false");
        }

        return config;
    }

    /* test */ static void addClusterToMirrorMaker2ConnectorConfig(Reconciliation reconciliation, Map<String, Object> config, KafkaMirrorMaker2ClusterSpec cluster, String configPrefix) {
        config.put(configPrefix + "alias", cluster.getAlias());
        config.put(configPrefix + AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());

        String securityProtocol = addTLSConfigToMirrorMaker2ConnectorConfig(config, cluster, configPrefix);

        if (cluster.getAuthentication() != null) {
            if (cluster.getAuthentication() instanceof KafkaClientAuthenticationTls) {
                config.put(configPrefix + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
                config.put(configPrefix + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, STORE_LOCATION_ROOT + cluster.getAlias() + KEYSTORE_SUFFIX);
                config.put(configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
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
    private static String addTLSConfigToMirrorMaker2ConnectorConfig(Map<String, Object> config, KafkaMirrorMaker2ClusterSpec cluster, String configPrefix) {
        if (cluster.getTls() != null) {
            if (cluster.getTls().getTrustedCertificates() != null && !cluster.getTls().getTrustedCertificates().isEmpty()) {
                config.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
                config.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, STORE_LOCATION_ROOT + cluster.getAlias() + TRUSTSTORE_SUFFIX);
                config.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, PLACEHOLDER_CERT_STORE_PASSWORD_CONFIG_PROVIDER_ENV_VAR);
            }

            return "SSL";
        } else {
            return "PLAINTEXT";
        }
    }
}
