/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.common.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.common.Rack;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorization;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationCustom;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationKeycloak;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationOpa;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfiguration;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationCustom;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.tieredstorage.RemoteStorageManager;
import io.strimzi.api.kafka.model.kafka.tieredstorage.TieredStorage;
import io.strimzi.api.kafka.model.kafka.tieredstorage.TieredStorageCustom;
import io.strimzi.kafka.oauth.server.ServerConfig;
import io.strimzi.kafka.oauth.server.plain.ServerPlainConfig;
import io.strimzi.operator.cluster.model.cruisecontrol.CruiseControlMetricsReporter;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlConfigurationParameters;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class is used to generate the broker configuration template. The template is later passed using a config map to
 * the broker pods. The scripts in the container images will fill in the variables in the template and use the
 * configuration file. This class is using the builder pattern to make it easy to test the different parts etc. To
 * generate the configuration file, it is using the PrintWriter.
 */
public class KafkaBrokerConfigurationBuilder {
    private final static String CONTROL_PLANE_LISTENER_NAME = "CONTROLPLANE-9090";
    private final static String REPLICATION_LISTENER_NAME = "REPLICATION-9091";

    // Names of environment variables placeholders replaced only in the running container
    private final static String PLACEHOLDER_CERT_STORE_PASSWORD = "${CERTS_STORE_PASSWORD}";
    private final static String PLACEHOLDER_RACK_ID = "${STRIMZI_RACK_ID}";
    private final static String PLACEHOLDER_OAUTH_CLIENT_SECRET = "${STRIMZI_%s_OAUTH_CLIENT_SECRET}";

    // Additional placeholders are used in the configuration file for node port addresses. These are used for both
    // per-broker and shared configurations. Listed here for reference only, they are constructed in the
    // KafkaAssemblyOperator class on the fly based on the user configuration.
    // * ${STRIMZI_NODEPORT_DEFAULT_ADDRESS}
    // * ${STRIMZI_NODEPORT_EXTERNALIP_ADDRESS}
    // * ${STRIMZI_NODEPORT_EXTERNALDNS_ADDRESS}
    // * ${STRIMZI_NODEPORT_INTERNALIP_ADDRESS}
    // * ${STRIMZI_NODEPORT_INTERNALDNS_ADDRESS}
    // * ${STRIMZI_NODEPORT_HOSTNAME_ADDRESS}

    private final StringWriter stringWriter = new StringWriter();
    private final PrintWriter writer = new PrintWriter(stringWriter);
    private final Reconciliation reconciliation;
    private final KafkaMetadataConfigurationState kafkaMetadataConfigState;
    private final NodeRef node;

    /**
     * Broker configuration template constructor
     *
     * @param reconciliation    The reconciliation
     * @param node              NodeRef instance
     * @param kafkaMetadataConfigState  Represents the state of the Kafka metadata configuration
     */
    public KafkaBrokerConfigurationBuilder(Reconciliation reconciliation, NodeRef node, KafkaMetadataConfigurationState kafkaMetadataConfigState) {
        printHeader();
        this.reconciliation = reconciliation;
        this.node = node;
        this.kafkaMetadataConfigState = kafkaMetadataConfigState;

        // Render the node/broker ID into the config file
        configureNodeOrBrokerId();
    }

    /**
     * Renders the broker.id or node.id configurations
     */
    private void configureNodeOrBrokerId()   {
        printSectionHeader("Node / Broker ID");

        // Node ID is ignored when not using Kraft mode => but it defaults to broker ID when not set.
        // We set it here in the configuration explicitly to avoid never ending rolling updates.
        writer.println("node.id=" + node.nodeId());
        // only broker in ZooKeeper-mode or during migration needs the Broker ID to be set
        if (node.broker() && kafkaMetadataConfigState.isZooKeeperToMigration()) {
            writer.println("broker.id=" + node.nodeId());
        }

        writer.println();
    }

    /**
     * Configures the Cruise Control metric reporter. It is set only if user enabled the Cruise Control.
     *
     * @param clusterName           Name of the Kafka cluster used to build the bootstrap address
     * @param ccMetricsReporter     Cruise Control Metrics Reporter configuration
     * @param isBroker              Flag indicating if this is broker (or controller)
     *
     * @return Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withCruiseControl(String clusterName, CruiseControlMetricsReporter ccMetricsReporter, boolean isBroker)   {
        if (ccMetricsReporter != null && isBroker) {
            printSectionHeader("Cruise Control configuration");
            writer.println(CruiseControlConfigurationParameters.METRICS_TOPIC_NAME + "=" + ccMetricsReporter.topicName());

            writer.println(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_ENDPOINT_ID_ALGO + "=HTTPS");
            // using the brokers service because the Admin client, in the Cruise Control metrics reporter, is not able to connect
            // to the pods behind the bootstrap one when they are not ready during startup.
            writer.println(CruiseControlConfigurationParameters.METRICS_REPORTER_BOOTSTRAP_SERVERS + "=" + KafkaResources.brokersServiceName(clusterName) + ":9091");
            writer.println(CruiseControlConfigurationParameters.METRICS_REPORTER_SECURITY_PROTOCOL + "=SSL");
            writer.println(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_TYPE + "=PKCS12");
            writer.println(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_LOCATION + "=/tmp/kafka/cluster.keystore.p12");
            writer.println(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_KEYSTORE_PASSWORD + "=" + PLACEHOLDER_CERT_STORE_PASSWORD);
            writer.println(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_TYPE + "=PKCS12");
            writer.println(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_LOCATION + "=/tmp/kafka/cluster.truststore.p12");
            writer.println(CruiseControlConfigurationParameters.METRICS_REPORTER_SSL_TRUSTSTORE_PASSWORD + "=" + PLACEHOLDER_CERT_STORE_PASSWORD);
            writer.println(CruiseControlConfigurationParameters.METRICS_TOPIC_AUTO_CREATE + "=true");

            if (ccMetricsReporter.numPartitions() != null) {
                writer.println(CruiseControlConfigurationParameters.METRICS_TOPIC_NUM_PARTITIONS + "=" + ccMetricsReporter.numPartitions());
            }

            if (ccMetricsReporter.replicationFactor() != null) {
                writer.println(CruiseControlConfigurationParameters.METRICS_TOPIC_REPLICATION_FACTOR + "=" + ccMetricsReporter.replicationFactor());
            }

            if (ccMetricsReporter.minInSyncReplicas() != null) {
                writer.println(CruiseControlConfigurationParameters.METRICS_TOPIC_MIN_ISR + "=" + ccMetricsReporter.minInSyncReplicas());
            }

            writer.println();
        }

        return this;
    }

    /**
     * Adds the template for the {@code rack.id}. The rack ID will be set in the container based on the value of the
     * {@code STRIMZI_RACK_ID} env var. It is set only if user enabled the rack awareness-
     *
     * @param rack The Rack Awareness configuration from the Kafka CR
     *
     * @return Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withRackId(Rack rack)   {
        if (rack != null) {
            printSectionHeader("Rack ID");
            writer.println("broker.rack=" + PLACEHOLDER_RACK_ID);
            writer.println();
        }

        return this;
    }

    /**
     * Configures the Zookeeper connection URL.
     *
     * @param clusterName The name of the Kafka custom resource
     *
     * @return Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withZookeeper(String clusterName)  {
        printSectionHeader("Zookeeper");
        writer.println(String.format("zookeeper.connect=%s:%d", KafkaResources.zookeeperServiceName(clusterName), ZookeeperCluster.CLIENT_TLS_PORT));
        writer.println("zookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty");
        writer.println("zookeeper.ssl.client.enable=true");
        writer.println("zookeeper.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
        writer.println("zookeeper.ssl.keystore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD);
        writer.println("zookeeper.ssl.keystore.type=PKCS12");
        writer.println("zookeeper.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
        writer.println("zookeeper.ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD);
        writer.println("zookeeper.ssl.truststore.type=PKCS12");
        writer.println();

        return this;
    }

    /**
     * Enable the ZooKeeper migration by setting the corresponding flag to true
     *
     * @return the builder instance
     */
    public KafkaBrokerConfigurationBuilder withZooKeeperMigration() {
        printSectionHeader("Zookeeper migration");
        writer.println("zookeeper.metadata.migration.enable=true");
        writer.println();

        return this;
    }

    /**
     * Adds the KRaft configuration for ZooKeeper-less Kafka. This includes the roles of the broker, the controller
     * listener name and the list of all controllers for quorum voting.
     *
     * @param clusterName   Name of the cluster (important for the advertised hostnames)
     * @param namespace     Namespace (important for generating the advertised hostname)
     * @param nodes         Set of node references for configuring the KRaft quorum
     *
     * @return Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withKRaft(String clusterName, String namespace, Set<NodeRef> nodes)   {
        printSectionHeader("KRaft configuration");
        if (node.controller() || (node.broker() && kafkaMetadataConfigState.isPostMigrationToKRaft())) {
            String roles = "broker,controller";
            if (node.broker() && !node.controller()) {
                roles = "broker";
            } else if (!node.broker()) {
                roles = "controller";
            }
            writer.println("process.roles=" + roles);
        }
        writer.println("controller.listener.names=" + CONTROL_PLANE_LISTENER_NAME);

        // Generates the controllers quorum list
        // The list should be sorted to avoid random changes to the generated configuration file
        List<String> quorum = nodes.stream()
                .filter(NodeRef::controller)
                .sorted(Comparator.comparingInt(NodeRef::nodeId))
                .map(node -> String.format("%s@%s:9090", node.nodeId(), DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(clusterName), node.podName())))
                .toList();

        writer.println("controller.quorum.voters=" + String.join(",", quorum));

        writer.println();

        return this;
    }

    /**
     * Configures the listeners based on the listeners enabled by the users in the Kafka CR. This method is used to
     * generate the per-broker configuration which uses actual broker IDs and addresses instead of just placeholders.
     *
     * @param clusterName                Name of the cluster (important for the advertised hostnames)
     * @param namespace                  Namespace (important for generating the advertised hostname)
     * @param kafkaListeners             The listeners configuration from the Kafka CR
     * @param advertisedHostnameProvider Lambda method which provides the advertised hostname for given listener and
     *                                   broker. This is used to configure the user-configurable listeners.
     * @param advertisedPortProvider     Lambda method which provides the advertised port for given listener and broker.
     *                                   This is used to configure the user-configurable listeners.
     *
     * @return Returns the builder instance
     */
    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    public KafkaBrokerConfigurationBuilder withListeners(
            String clusterName,
            String namespace,
            List<GenericKafkaListener> kafkaListeners,
            Function<String, String> advertisedHostnameProvider,
            Function<String, String> advertisedPortProvider
    )  {
        List<String> listeners = new ArrayList<>();
        List<String> advertisedListeners = new ArrayList<>();
        List<String> securityProtocol = new ArrayList<>();

        boolean isKraftControllerOnly = node.controller() && !node.broker();

        // Control Plane listener is set for pure KRaft controller or combined node, and broker in ZooKeeper mode or in migration state but not when full KRaft.
        if (node.controller() || (node.broker() && kafkaMetadataConfigState.isZooKeeperToMigration())) {
            listeners.add(CONTROL_PLANE_LISTENER_NAME + "://0.0.0.0:9090");

            // Control Plane listener to be advertised only with broker in ZooKeeper-based or migration but NOT when full KRaft only or mixed
            if (node.broker() && kafkaMetadataConfigState.isZooKeeperToMigration()) {
                advertisedListeners.add(String.format("%s://%s:9090",
                        CONTROL_PLANE_LISTENER_NAME,
                        // Pod name constructed to be templatable for each individual ordinal
                        DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace, KafkaResources.brokersServiceName(clusterName), node.podName())
                ));
            }
        }

        // Security protocol and Control Plane Listener are configured everywhere

        // Brokers need to know how to connect to the controllers on the Control Plane listener and what security (encryption/authentication) they should use.
        // For that reason, we have to configure the Control Plane listener in the broker-only configuration as well,
        // even though they do not listen at the Control Plane listener port.
        // The brokers use this configuration to detect how to connect to the controllers, what certificates to use etc.
        securityProtocol.add(CONTROL_PLANE_LISTENER_NAME + ":SSL");
        // Control Plane listener is configured on KRaft broker only nodes as well for allowing TLS certificates keystore generation
        // so that brokers are able to connect to controllers as TLS clients
        configureControlPlaneListener();

        // Replication Listener to be configured on brokers and KRaft controllers only but until post-migration
        if (node.broker() || node.controller() && kafkaMetadataConfigState.isZooKeeperToPostMigration()) {
            securityProtocol.add(REPLICATION_LISTENER_NAME + ":SSL");
            configureReplicationListener();
        }

        // Non-controller listeners are used only on ZooKeeper based brokers or KRaft brokers (including mixed nodes)
        if (!isKraftControllerOnly) {
            // Replication listener
            listeners.add(REPLICATION_LISTENER_NAME + "://0.0.0.0:9091");
            advertisedListeners.add(String.format("%s://%s:9091",
                    REPLICATION_LISTENER_NAME,
                    // Pod name constructed to be templatable for each individual ordinal
                    DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace, KafkaResources.brokersServiceName(clusterName), node.podName())
            ));

            for (GenericKafkaListener listener : kafkaListeners) {
                int port = listener.getPort();
                String listenerName = ListenersUtils.identifier(listener).toUpperCase(Locale.ENGLISH);
                String envVarListenerName = ListenersUtils.envVarIdentifier(listener);

                printSectionHeader("Listener configuration: " + listenerName);

                listeners.add(listenerName + "://0.0.0.0:" + port);
                advertisedListeners.add(String.format("%s://%s:%s", listenerName, advertisedHostnameProvider.apply(envVarListenerName), advertisedPortProvider.apply(envVarListenerName)));
                configureAuthentication(listenerName, securityProtocol, listener.isTls(), listener.getAuth());
                configureListener(listenerName, listener.getConfiguration());

                if (listener.isTls()) {
                    CertAndKeySecretSource customServerCert = null;
                    if (listener.getConfiguration() != null) {
                        customServerCert = listener.getConfiguration().getBrokerCertChainAndKey();
                    }

                    configureTls(listenerName, customServerCert);
                }

                writer.println();
            }
        }

        // configure OAuth principal builder for all the nodes - brokers, controllers, and mixed
        configureOAuthPrincipalBuilderIfNeeded(writer, kafkaListeners);

        printSectionHeader("Common listener configuration");
        writer.println("listener.security.protocol.map=" + String.join(",", securityProtocol));
        writer.println("listeners=" + String.join(",", listeners));

        // Advertised listeners are not allowed on KRaft nodes with controller only role
        if (!isKraftControllerOnly) {
            writer.println("advertised.listeners=" + String.join(",", advertisedListeners));
            writer.println("inter.broker.listener.name=" + REPLICATION_LISTENER_NAME);
        } else if (node.controller() && kafkaMetadataConfigState.isZooKeeperToPostMigration()) {
            // needed for KRaft controller only as well until post-migration because it needs to contact brokers
            writer.println("inter.broker.listener.name=" + REPLICATION_LISTENER_NAME);
        }

        // Control plane listener is on all ZooKeeper based brokers, needed during migration as well, when broker still using ZooKeeper but KRaft controllers are ready
        if (node.broker() && kafkaMetadataConfigState.isZooKeeperToMigration()) {
            writer.println("control.plane.listener.name=" + CONTROL_PLANE_LISTENER_NAME);
        }

        writer.println("sasl.enabled.mechanisms=");
        writer.println("ssl.endpoint.identification.algorithm=HTTPS");
        writer.println();

        return this;
    }

    private void configureOAuthPrincipalBuilderIfNeeded(PrintWriter writer, List<GenericKafkaListener> kafkaListeners) {
        for (GenericKafkaListener listener : kafkaListeners) {
            if (listener.getAuth() instanceof KafkaListenerAuthenticationOAuth) {
                writer.println(String.format("principal.builder.class=%s", KafkaListenerAuthenticationOAuth.PRINCIPAL_BUILDER_CLASS_NAME));
                writer.println();
                return;
            }
        }
    }

    /**
     * Internal method which configures the control plane listener. The control plane listener configuration is currently
     * rather static, it always uses TLS with TLS client auth.
     */
    private void configureControlPlaneListener() {
        final String controlPlaneListenerName = CONTROL_PLANE_LISTENER_NAME.toLowerCase(Locale.ENGLISH);

        printSectionHeader("Control Plane listener");
        configureListener(controlPlaneListenerName);
    }

    /**
     * Internal method which configures the replication listener. The replication listener configuration is currently
     * rather static, it always uses TLS with TLS client auth.
     */
    private void configureReplicationListener() {
        final String replicationListenerName = REPLICATION_LISTENER_NAME.toLowerCase(Locale.ENGLISH);

        printSectionHeader("Replication listener");
        configureListener(replicationListenerName);
    }

    /**
     * Internal method which generates the configuration of the internal replication or control plane listener which use
     * the same TLS configuration and differ only on the listener name.
     *
     * @param listenerName  Name of the listener
     */
    private void configureListener(String listenerName) {
        writer.println("listener.name." + listenerName + ".ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
        writer.println("listener.name." + listenerName + ".ssl.keystore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD);
        writer.println("listener.name." + listenerName + ".ssl.keystore.type=PKCS12");
        writer.println("listener.name." + listenerName + ".ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
        writer.println("listener.name." + listenerName + ".ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD);
        writer.println("listener.name." + listenerName + ".ssl.truststore.type=PKCS12");
        writer.println("listener.name." + listenerName + ".ssl.client.auth=required");
        writer.println();
    }

    /**
     * Configures the listener. This method is used only internally.
     *
     * @param listenerName  The name of the listener as it is referenced in the Kafka broker configuration file
     * @param configuration The configuration of the listener (null if not specified by the user in the Kafka CR)
     */
    private void configureListener(String listenerName, GenericKafkaListenerConfiguration configuration) {
        if (configuration != null)  {
            String listenerNameInProperty = listenerName.toLowerCase(Locale.ENGLISH);

            if (configuration.getMaxConnections() != null)  {
                writer.println(String.format("listener.name.%s.max.connections=%d", listenerNameInProperty, configuration.getMaxConnections()));
            }

            if (configuration.getMaxConnectionCreationRate() != null)  {
                writer.println(String.format("listener.name.%s.max.connection.creation.rate=%d", listenerNameInProperty, configuration.getMaxConnectionCreationRate()));
            }
        }
    }

    /**
     * Configures TLS for a specific listener. This method is used only internally.
     *
     * @param listenerName  The name of the listener under which it is used in the Kafka broker configuration file
     * @param serverCertificate The custom certificate configuration (null if not specified by the user in the Kafka CR)
     */
    private void configureTls(String listenerName, CertAndKeySecretSource serverCertificate) {
        String listenerNameInProperty = listenerName.toLowerCase(Locale.ENGLISH);

        if (serverCertificate != null)  {
            writer.println(String.format("listener.name.%s.ssl.keystore.location=/tmp/kafka/custom-%s.keystore.p12", listenerNameInProperty, listenerNameInProperty));
        } else {
            writer.println(String.format("listener.name.%s.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12", listenerNameInProperty));
        }

        writer.println(String.format("listener.name.%s.ssl.keystore.password=%s", listenerNameInProperty, PLACEHOLDER_CERT_STORE_PASSWORD));
        writer.println(String.format("listener.name.%s.ssl.keystore.type=PKCS12", listenerNameInProperty));

        writer.println();
    }

    /**
     * Configures authentication for a Kafka listener. This method is used only internally.
     *
     * @param listenerName  Name of the listener as used in the Kafka broker configuration file.
     * @param securityProtocol  List of security protocols enabled in the broker. The method will add the security
     *                          protocol configuration for this listener to this list (e.g. SASL_PLAINTEXT).
     * @param tls   Flag whether this protocol is using TLS or not
     * @param auth  The authentication configuration from the Kafka CR
     */
    private void configureAuthentication(String listenerName, List<String> securityProtocol, boolean tls, KafkaListenerAuthentication auth)    {
        String listenerNameInProperty = listenerName.toLowerCase(Locale.ENGLISH);
        String listenerNameInEnvVar = listenerName.replace("-", "_");

        if (auth instanceof KafkaListenerAuthenticationOAuth oauth) {
            securityProtocol.add(String.format("%s:%s", listenerName, getSecurityProtocol(tls, true)));

            Map<String, String> jaasOptions = new LinkedHashMap<>(getOAuthOptions(oauth));
            addOptionIfNotNull(jaasOptions, "oauth.config.id", listenerName);

            if (oauth.getClientSecret() != null)    {
                addOptionIfNotNull(jaasOptions, "oauth.client.secret", String.format(PLACEHOLDER_OAUTH_CLIENT_SECRET, listenerNameInEnvVar));
            }

            if (oauth.getTlsTrustedCertificates() != null && oauth.getTlsTrustedCertificates().size() > 0)    {
                addOptionIfNotNull(jaasOptions, "oauth.ssl.truststore.location", String.format("/tmp/kafka/oauth-%s.truststore.p12", listenerNameInProperty));
                addOptionIfNotNull(jaasOptions, "oauth.ssl.truststore.password", PLACEHOLDER_CERT_STORE_PASSWORD);
                addOptionIfNotNull(jaasOptions, "oauth.ssl.truststore.type", "PKCS12");
            }

            StringBuilder enabledMechanisms = new StringBuilder();
            if (oauth.isEnableOauthBearer()) {
                writer.println(String.format("listener.name.%s.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler", listenerNameInProperty));
                var oauthBearerOptions = new LinkedHashMap<String, String>();
                addOptionIfNotNull(oauthBearerOptions, "unsecuredLoginStringClaim_sub", "thePrincipalName");
                oauthBearerOptions.putAll(jaasOptions);
                writer.println(String.format("listener.name.%s.oauthbearer.sasl.jaas.config=%s", listenerNameInProperty,
                        AuthenticationUtils.jaasConfig("org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule", oauthBearerOptions)));
                enabledMechanisms.append("OAUTHBEARER");
            }

            if (oauth.isEnablePlain()) {
                addOptionIfNotNull(jaasOptions, ServerPlainConfig.OAUTH_TOKEN_ENDPOINT_URI, oauth.getTokenEndpointUri());
                writer.println(String.format("listener.name.%s.plain.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.plain.JaasServerOauthOverPlainValidatorCallbackHandler", listenerNameInProperty));
                writer.println(String.format("listener.name.%s.plain.sasl.jaas.config=%s", listenerNameInProperty,
                        AuthenticationUtils.jaasConfig("org.apache.kafka.common.security.plain.PlainLoginModule", jaasOptions)));
                if (enabledMechanisms.length() > 0) {
                    enabledMechanisms.append(",");
                }
                enabledMechanisms.append("PLAIN");

            }

            writer.println(String.format("listener.name.%s.sasl.enabled.mechanisms=%s", listenerNameInProperty, enabledMechanisms));

            if (oauth.getMaxSecondsWithoutReauthentication() != null) {
                writer.println(String.format("listener.name.%s.connections.max.reauth.ms=%s", listenerNameInProperty, 1000 * oauth.getMaxSecondsWithoutReauthentication()));
            }
            writer.println();
        } else if (auth instanceof KafkaListenerAuthenticationScramSha512) {
            securityProtocol.add(String.format("%s:%s", listenerName, getSecurityProtocol(tls, true)));
            writer.println(String.format("listener.name.%s.scram-sha-512.sasl.jaas.config=%s", listenerNameInProperty,
                    AuthenticationUtils.jaasConfig("org.apache.kafka.common.security.scram.ScramLoginModule", Map.of())));
            writer.println(String.format("listener.name.%s.sasl.enabled.mechanisms=SCRAM-SHA-512", listenerNameInProperty));
            writer.println();
        } else if (auth instanceof KafkaListenerAuthenticationTls) {
            securityProtocol.add(String.format("%s:%s", listenerName, getSecurityProtocol(tls, false)));

            writer.println(String.format("listener.name.%s.ssl.client.auth=required", listenerNameInProperty));
            writer.println(String.format("listener.name.%s.ssl.truststore.location=/tmp/kafka/clients.truststore.p12", listenerNameInProperty));
            writer.println(String.format("listener.name.%s.ssl.truststore.password=%s", listenerNameInProperty, PLACEHOLDER_CERT_STORE_PASSWORD));
            writer.println(String.format("listener.name.%s.ssl.truststore.type=PKCS12", listenerNameInProperty));
            writer.println();
        } else if (auth instanceof KafkaListenerAuthenticationCustom customAuth) {
            securityProtocol.add(String.format("%s:%s", listenerName, getSecurityProtocol(tls, customAuth.isSasl())));
            Map<String, Object> listenerConfig = customAuth.getListenerConfig();
            if (listenerConfig == null) {
                listenerConfig = new HashMap<String, Object>();
            }
            KafkaListenerCustomAuthConfiguration config = new KafkaListenerCustomAuthConfiguration(reconciliation, listenerConfig.entrySet());
            config.asOrderedProperties().asMap().forEach((key, value) -> writer.println(String.format("listener.name.%s.%s=%s", listenerNameInProperty, key, value)));
        } else {
            securityProtocol.add(String.format("%s:%s", listenerName, getSecurityProtocol(tls, false)));
        }
    }

    /**
     * Generates the security protocol
     *
     * @param tls  Flag whether TLS is enabled
     * @param sasl  Flag whether SASL is enabled
     *
     * @return String with the security protocol
     */
    private String getSecurityProtocol(boolean tls, boolean sasl)   {
        String a = tls ? "SSL" : "PLAINTEXT";
        return sasl ? "SASL_" + a : a;
    }

    /**
     * Generates the public part of the OAUTH configuration for JAAS. The private part is not added here but mounted as
     * a secret reference to keep it secure. This is only internal method.
     *
     * @param oauth     OAuth type authentication object
     * @return  Returns the builder instance
     */
    /*test*/ static Map<String, String> getOAuthOptions(KafkaListenerAuthenticationOAuth oauth)  {
        Map<String, String> options = new LinkedHashMap<>();

        addOptionIfNotNull(options, ServerConfig.OAUTH_CLIENT_ID, oauth.getClientId());
        addOptionIfNotNull(options, ServerConfig.OAUTH_VALID_ISSUER_URI, oauth.getValidIssuerUri());
        addBooleanOptionIfFalse(options, ServerConfig.OAUTH_CHECK_ISSUER, oauth.isCheckIssuer());
        addBooleanOptionIfTrue(options, ServerConfig.OAUTH_CHECK_AUDIENCE, oauth.isCheckAudience());
        addOptionIfNotNull(options, ServerConfig.OAUTH_CUSTOM_CLAIM_CHECK, oauth.getCustomClaimCheck());
        addOptionIfNotNull(options, ServerConfig.OAUTH_SCOPE, oauth.getClientScope());
        addOptionIfNotNull(options, ServerConfig.OAUTH_AUDIENCE, oauth.getClientAudience());
        addOptionIfNotNull(options, ServerConfig.OAUTH_JWKS_ENDPOINT_URI, oauth.getJwksEndpointUri());
        if (oauth.getJwksRefreshSeconds() != null && oauth.getJwksRefreshSeconds() > 0) {
            addOptionIfNotNull(options, ServerConfig.OAUTH_JWKS_REFRESH_SECONDS, String.valueOf(oauth.getJwksRefreshSeconds()));
        }
        if (oauth.getJwksRefreshSeconds() != null && oauth.getJwksExpirySeconds() > 0) {
            addOptionIfNotNull(options, ServerConfig.OAUTH_JWKS_EXPIRY_SECONDS, String.valueOf(oauth.getJwksExpirySeconds()));
        }
        if (oauth.getJwksMinRefreshPauseSeconds() != null && oauth.getJwksMinRefreshPauseSeconds() >= 0) {
            addOptionIfNotNull(options, ServerConfig.OAUTH_JWKS_REFRESH_MIN_PAUSE_SECONDS, String.valueOf(oauth.getJwksMinRefreshPauseSeconds()));
        }
        addBooleanOptionIfTrue(options, ServerConfig.OAUTH_JWKS_IGNORE_KEY_USE, oauth.getJwksIgnoreKeyUse());
        addOptionIfNotNull(options, ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, oauth.getIntrospectionEndpointUri());
        addOptionIfNotNull(options, ServerConfig.OAUTH_USERINFO_ENDPOINT_URI, oauth.getUserInfoEndpointUri());
        addOptionIfNotNull(options, ServerConfig.OAUTH_USERNAME_CLAIM, oauth.getUserNameClaim());
        addOptionIfNotNull(options, ServerConfig.OAUTH_FALLBACK_USERNAME_CLAIM, oauth.getFallbackUserNameClaim());
        addOptionIfNotNull(options, ServerConfig.OAUTH_FALLBACK_USERNAME_PREFIX, oauth.getFallbackUserNamePrefix());
        addOptionIfNotNull(options, ServerConfig.OAUTH_GROUPS_CLAIM, oauth.getGroupsClaim());
        addOptionIfNotNull(options, ServerConfig.OAUTH_GROUPS_CLAIM_DELIMITER, oauth.getGroupsClaimDelimiter());
        addBooleanOptionIfFalse(options, ServerConfig.OAUTH_ACCESS_TOKEN_IS_JWT, oauth.isAccessTokenIsJwt());
        addBooleanOptionIfFalse(options, ServerConfig.OAUTH_CHECK_ACCESS_TOKEN_TYPE, oauth.isCheckAccessTokenType());
        addOptionIfNotNull(options, ServerConfig.OAUTH_VALID_TOKEN_TYPE, oauth.getValidTokenType());

        if (oauth.isDisableTlsHostnameVerification()) {
            addOptionIfNotNull(options, ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");
        }

        if (oauth.getConnectTimeoutSeconds() != null && oauth.getConnectTimeoutSeconds() > 0) {
            addOptionIfNotNull(options, ServerConfig.OAUTH_CONNECT_TIMEOUT_SECONDS, String.valueOf(oauth.getConnectTimeoutSeconds()));
        }
        if (oauth.getReadTimeoutSeconds() != null && oauth.getReadTimeoutSeconds() > 0) {
            addOptionIfNotNull(options, ServerConfig.OAUTH_READ_TIMEOUT_SECONDS, String.valueOf(oauth.getReadTimeoutSeconds()));
        }
        if (oauth.getHttpRetries() != null && oauth.getHttpRetries() > 0) {
            addOptionIfNotNull(options, ServerConfig.OAUTH_HTTP_RETRIES, String.valueOf(oauth.getHttpRetries()));
        }
        if (oauth.getHttpRetryPauseMs() != null && oauth.getHttpRetryPauseMs() > 0) {
            addOptionIfNotNull(options, ServerConfig.OAUTH_HTTP_RETRY_PAUSE_MILLIS, String.valueOf(oauth.getHttpRetryPauseMs()));
        }

        addBooleanOptionIfTrue(options, ServerConfig.OAUTH_ENABLE_METRICS, oauth.isEnableMetrics());
        addBooleanOptionIfFalse(options, ServerConfig.OAUTH_FAIL_FAST, oauth.getFailFast());
        addBooleanOptionIfFalse(options, ServerConfig.OAUTH_INCLUDE_ACCEPT_HEADER, oauth.isIncludeAcceptHeader());

        return options;
    }

    static void addOptionIfNotNull(Map<String, String> options, String option, String value) {
        if (value != null) {
            options.put(option, value);
        }
    }

    static void addBooleanOptionIfTrue(Map<String, String> options, String option, boolean value) {
        if (value) {
            options.put(option, String.valueOf(true));
        }
    }

    static void addBooleanOptionIfFalse(Map<String, String> options, String option, boolean value) {
        if (!value) {
            options.put(option, String.valueOf(false));
        }
    }

    static void addOptionIfNotNull(PrintWriter writer, String name, Object value) {
        if (value != null) {
            writer.println(name + "=" + value);
        }
    }

    /**
     * Configures authorization for the Kafka cluster.
     *
     * @param clusterName   The name of the cluster (used to configure the replication super-users)
     * @param authorization The authorization configuration from the Kafka CR
     *
     * @return  Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withAuthorization(String clusterName, KafkaAuthorization authorization)  {
        if (authorization != null) {
            List<String> superUsers = new ArrayList<>();

            // Broker super users
            superUsers.add(String.format("User:CN=%s,O=io.strimzi", KafkaResources.kafkaComponentName(clusterName)));
            superUsers.add(String.format("User:CN=%s-%s,O=io.strimzi", clusterName, "entity-topic-operator"));
            superUsers.add(String.format("User:CN=%s-%s,O=io.strimzi", clusterName, "entity-user-operator"));
            superUsers.add(String.format("User:CN=%s-%s,O=io.strimzi", clusterName, "kafka-exporter"));
            superUsers.add(String.format("User:CN=%s-%s,O=io.strimzi", clusterName, "cruise-control"));

            superUsers.add(String.format("User:CN=%s,O=io.strimzi", "cluster-operator"));

            printSectionHeader("Authorization");
            // when the StandardAuthorizer has to replace the AclAuthorizer during the migration depending on nodes in full KRaft
            boolean useKRaft = (node.controller() && kafkaMetadataConfigState.isPreMigrationToKRaft()) ||
                    (node.broker() && kafkaMetadataConfigState.isPostMigrationToKRaft());
            configureAuthorization(clusterName, superUsers, authorization, useKRaft);
            writer.println("super.users=" + String.join(";", superUsers));
            writer.println();
        }

        return this;
    }

    /**
     * Configures authorization for the Kafka brokers. This method is used only internally.
     *
     * @param clusterName Name of the cluster
     * @param superUsers Super-users list who have all the rights on the cluster
     * @param authorization The authorization configuration from the Kafka CR
     * @param useKRaft      Use KRaft mode in the configuration
     */
    private void configureAuthorization(String clusterName, List<String> superUsers, KafkaAuthorization authorization, boolean useKRaft) {
        if (authorization instanceof KafkaAuthorizationSimple simpleAuthz) {
            configureSimpleAuthorization(simpleAuthz, superUsers, useKRaft);
        } else if (authorization instanceof KafkaAuthorizationOpa opaAuthz) {
            configureOpaAuthorization(opaAuthz, superUsers);
        } else if (authorization instanceof KafkaAuthorizationKeycloak keycloakAuthz) {
            configureKeycloakAuthorization(clusterName, keycloakAuthz, superUsers);
        } else if (authorization instanceof KafkaAuthorizationCustom customAuthz) {
            configureCustomAuthorization(customAuthz, superUsers);
        }
    }

    /**
     * Configures Simple authorization
     *
     * @param authorization     Simple authorization configuration
     * @param superUsers        Super-users list who have all the rights on the cluster
     * @param useKRaft          Use KRaft mode in the configuration
     */
    private void configureSimpleAuthorization(KafkaAuthorizationSimple authorization, List<String> superUsers, boolean useKRaft) {
        if (useKRaft) {
            writer.println("authorizer.class.name=" + KafkaAuthorizationSimple.KRAFT_AUTHORIZER_CLASS_NAME);
        } else {
            writer.println("authorizer.class.name=" + KafkaAuthorizationSimple.AUTHORIZER_CLASS_NAME);
        }

        // User configured super-users
        if (authorization.getSuperUsers() != null && authorization.getSuperUsers().size() > 0) {
            superUsers.addAll(authorization.getSuperUsers().stream().map(e -> String.format("User:%s", e)).toList());
        }
    }

    /**
     * Configures Open Policy Agent (OPA) authorization
     *
     * @param authorization     OPA authorization configuration
     * @param superUsers        Super-users list who have all the rights on the cluster
     */
    private void configureOpaAuthorization(KafkaAuthorizationOpa authorization, List<String> superUsers) {
        writer.println("authorizer.class.name=" + KafkaAuthorizationOpa.AUTHORIZER_CLASS_NAME);

        writer.println(String.format("%s=%s", "opa.authorizer.url", authorization.getUrl()));
        writer.println(String.format("%s=%b", "opa.authorizer.allow.on.error", authorization.isAllowOnError()));
        writer.println(String.format("%s=%b", "opa.authorizer.metrics.enabled", authorization.isEnableMetrics()));
        writer.println(String.format("%s=%d", "opa.authorizer.cache.initial.capacity", authorization.getInitialCacheCapacity()));
        writer.println(String.format("%s=%d", "opa.authorizer.cache.maximum.size", authorization.getMaximumCacheSize()));
        writer.println(String.format("%s=%d", "opa.authorizer.cache.expire.after.seconds", Duration.ofMillis(authorization.getExpireAfterMs()).getSeconds()));

        if (authorization.getTlsTrustedCertificates() != null && authorization.getTlsTrustedCertificates().size() > 0)    {
            writer.println("opa.authorizer.truststore.path=/tmp/kafka/authz-opa.truststore.p12");
            writer.println("opa.authorizer.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD);
            writer.println("opa.authorizer.truststore.type=PKCS12");
        }

        // User configured super-users
        if (authorization.getSuperUsers() != null && authorization.getSuperUsers().size() > 0) {
            superUsers.addAll(authorization.getSuperUsers().stream().map(e -> String.format("User:%s", e)).toList());
        }
    }

    /**
     * Configures Keycloak authorization
     *
     * @param clusterName       Name of the cluster
     * @param authorization     Custom authorization configuration
     * @param superUsers        Super-users list who have all the rights on the cluster
     */
    private void configureKeycloakAuthorization(String clusterName, KafkaAuthorizationKeycloak authorization, List<String> superUsers) {
        writer.println("authorizer.class.name=" + KafkaAuthorizationKeycloak.AUTHORIZER_CLASS_NAME);
        writer.println("strimzi.authorization.token.endpoint.uri=" + authorization.getTokenEndpointUri());
        writer.println("strimzi.authorization.client.id=" + authorization.getClientId());
        writer.println("strimzi.authorization.delegate.to.kafka.acl=" + authorization.isDelegateToKafkaAcls());
        addOptionIfNotNull(writer, "strimzi.authorization.grants.refresh.period.seconds", authorization.getGrantsRefreshPeriodSeconds());
        addOptionIfNotNull(writer, "strimzi.authorization.grants.refresh.pool.size", authorization.getGrantsRefreshPoolSize());
        addOptionIfNotNull(writer, "strimzi.authorization.grants.max.idle.time.seconds", authorization.getGrantsMaxIdleTimeSeconds());
        addOptionIfNotNull(writer, "strimzi.authorization.grants.gc.period.seconds", authorization.getGrantsGcPeriodSeconds());
        addOptionIfNotNull(writer, "strimzi.authorization.connect.timeout.seconds", authorization.getConnectTimeoutSeconds());
        addOptionIfNotNull(writer, "strimzi.authorization.read.timeout.seconds", authorization.getReadTimeoutSeconds());
        addOptionIfNotNull(writer, "strimzi.authorization.http.retries", authorization.getHttpRetries());
        if (authorization.isGrantsAlwaysLatest()) {
            writer.println("strimzi.authorization.reuse.grants=false");
        }
        if (authorization.isEnableMetrics()) {
            writer.println("strimzi.authorization.enable.metrics=true");
        }
        if (!authorization.isIncludeAcceptHeader()) {
            writer.println("strimzi.authorization.include.accept.header=false");
        }

        writer.println("strimzi.authorization.kafka.cluster.name=" + clusterName);

        if (authorization.getTlsTrustedCertificates() != null && authorization.getTlsTrustedCertificates().size() > 0)    {
            writer.println("strimzi.authorization.ssl.truststore.location=/tmp/kafka/authz-keycloak.truststore.p12");
            writer.println("strimzi.authorization.ssl.truststore.password=" + PLACEHOLDER_CERT_STORE_PASSWORD);
            writer.println("strimzi.authorization.ssl.truststore.type=PKCS12");
            String endpointIdentificationAlgorithm = authorization.isDisableTlsHostnameVerification() ? "" : "HTTPS";
            writer.println("strimzi.authorization.ssl.endpoint.identification.algorithm=" + endpointIdentificationAlgorithm);
        }

        // User configured super-users
        if (authorization.getSuperUsers() != null && authorization.getSuperUsers().size() > 0) {
            superUsers.addAll(authorization.getSuperUsers().stream().map(e -> String.format("User:%s", e)).toList());
        }
    }

    /**
     * Configures custom authorization
     *
     * @param authorization     Custom authorization configuration
     * @param superUsers        Super-users list who have all the rights on the cluster
     */
    private void configureCustomAuthorization(KafkaAuthorizationCustom authorization, List<String> superUsers) {
        writer.println("authorizer.class.name=" + authorization.getAuthorizerClass());

        // User configured super-users
        if (authorization.getSuperUsers() != null && authorization.getSuperUsers().size() > 0) {
            superUsers.addAll(authorization.getSuperUsers().stream().map(e -> String.format("User:%s", e)).toList());
        }
    }

    /**
     * Configures the configuration options passed by the user in the Kafka CR.
     *
     * @param userConfig                The User configuration - Kafka broker configuration options specified by the user in the Kafka custom resource
     * @param injectCcMetricsReporter   Inject the Cruise Control Metrics Reporter into the configuration
     *
     * @return Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withUserConfiguration(KafkaConfiguration userConfig, boolean injectCcMetricsReporter)  {
        if (userConfig != null && !userConfig.getConfiguration().isEmpty()) {
            if (injectCcMetricsReporter)  {
                // We have to create a copy of the configuration before we modify it
                userConfig = new KafkaConfiguration(userConfig);

                // We configure the Cruise Control Metrics Reporter is needed
                if (userConfig.getConfigOption(CruiseControlMetricsReporter.KAFKA_METRIC_REPORTERS_CONFIG_FIELD) != null) {
                    if (!userConfig.getConfigOption(CruiseControlMetricsReporter.KAFKA_METRIC_REPORTERS_CONFIG_FIELD).contains(CruiseControlMetricsReporter.CRUISE_CONTROL_METRIC_REPORTER)) {
                        userConfig.setConfigOption(CruiseControlMetricsReporter.KAFKA_METRIC_REPORTERS_CONFIG_FIELD, userConfig.getConfigOption(CruiseControlMetricsReporter.KAFKA_METRIC_REPORTERS_CONFIG_FIELD) + "," + CruiseControlMetricsReporter.CRUISE_CONTROL_METRIC_REPORTER);
                    }
                } else {
                    userConfig.setConfigOption(CruiseControlMetricsReporter.KAFKA_METRIC_REPORTERS_CONFIG_FIELD, CruiseControlMetricsReporter.CRUISE_CONTROL_METRIC_REPORTER);
                }
            }

            printSectionHeader("User provided configuration");
            writer.println(userConfig.getConfiguration());
            writer.println();
        } else if (injectCcMetricsReporter) {
            // There is no user provided configuration. But we still need to inject the Cruise Control Metrics Reporter
            printSectionHeader("Cruise Control Metrics Reporter");
            writer.println(CruiseControlMetricsReporter.KAFKA_METRIC_REPORTERS_CONFIG_FIELD + "=" + CruiseControlMetricsReporter.CRUISE_CONTROL_METRIC_REPORTER);
            writer.println();
        }

        return this;
    }

    /**
     * Configures the log dirs used by the Kafka brokers. The log dirs contain a broker ID in the path. This is passed
     * as template and filled in only in the Kafka container.
     *
     * @param mounts    List of data volume mounts which mount the data volumes into the container
     *
     * @return  Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withLogDirs(List<VolumeMount> mounts)  {
        // We take all the data mount points and add the broker specific path
        String logDirs = mounts.stream()
                .map(volumeMount -> volumeMount.getMountPath() + "/kafka-log" + node.nodeId()).collect(Collectors.joining(","));

        printSectionHeader("Kafka message logs configuration");
        writer.println("log.dirs=" + logDirs);
        writer.println();

        return this;
    }

    /**
     * Configure the tiered storage configuration for Kafka brokers.
     *
     * @param clusterName     Name of the cluster
     * @param tieredStorage   TieredStorage configuration.
     *
     * @return  Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withTieredStorage(String clusterName, TieredStorage tieredStorage)  {
        if (tieredStorage == null) {
            return this;
        }

        printSectionHeader("Kafka tiered storage configuration");
        writer.println("# RLMM configuration generated by Strimzi");

        writer.println("remote.log.storage.system.enable=true");
        writer.println("remote.log.metadata.manager.impl.prefix=rlmm.config.");
        writer.println("remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager");
        writer.println("remote.log.metadata.manager.listener.name=" + REPLICATION_LISTENER_NAME);
        writer.println("rlmm.config.remote.log.metadata.common.client.bootstrap.servers="
            + clusterName + "-kafka-brokers:9091");
        writer.println("rlmm.config.remote.log.metadata.common.client.security.protocol=SSL");
        writer.println("rlmm.config.remote.log.metadata.common.client.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
        writer.println("rlmm.config.remote.log.metadata.common.client.ssl.keystore.password=${CERTS_STORE_PASSWORD}");
        writer.println("rlmm.config.remote.log.metadata.common.client.ssl.keystore.type=PKCS12");
        writer.println("rlmm.config.remote.log.metadata.common.client.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
        writer.println("rlmm.config.remote.log.metadata.common.client.ssl.truststore.password=${CERTS_STORE_PASSWORD}");
        writer.println("rlmm.config.remote.log.metadata.common.client.ssl.truststore.type=PKCS12");

        writer.println("# RSM configs set by the operator and by the user");

        if (tieredStorage instanceof TieredStorageCustom customTieredStorage) {
            RemoteStorageManager rsm = customTieredStorage.getRemoteStorageManager();
            writer.println("remote.log.storage.manager.class.name=" + rsm.getClassName());
            writer.println("remote.log.storage.manager.class.path=" + rsm.getClassPath());
            writer.println("remote.log.storage.manager.impl.prefix=rsm.config.");

            for (Map.Entry<String, String> config : rsm.getConfig().entrySet()) {
                writer.println(String.format("rsm.config.%s=%s", config.getKey(), config.getValue()));
            }
        }

        writer.println();

        return this;
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
     * Generates the configuration template as String
     *
     * @return String with the Kafka broker configuration template
     */
    public String build()  {
        return stringWriter.toString();
    }
}
