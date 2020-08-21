/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.CruiseControlSpec;
import io.strimzi.api.kafka.model.KafkaAuthorization;
import io.strimzi.api.kafka.model.KafkaAuthorizationKeycloak;
import io.strimzi.api.kafka.model.KafkaAuthorizationOpa;
import io.strimzi.api.kafka.model.KafkaAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.Rack;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternal;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalIngress;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalLoadBalancer;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePort;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalRoute;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.kafka.oauth.server.ServerConfig;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * This class is used to generate the broker configuration template. The template is later passed using a config map to
 * the broker pods. The scripts in the container images will fill in the variables in the template and use the
 * configuration file. This class is using the builder pattern to make it easy to test the different parts etc. To
 * generate the configuration file, it is using the PrintWriter.
 */
public class KafkaBrokerConfigurationBuilder {
    private final StringWriter stringWriter = new StringWriter();
    private final PrintWriter writer = new PrintWriter(stringWriter);

    /**
     * Broker configuration template constructor
     */
    public KafkaBrokerConfigurationBuilder() {
        printHeader();
    }

    /**
     * Adds the broker ID template. The actual broker ID will be replaced in the container from the
     * {@code STRIMZI_BROKER_ID} environment variable.
     *
     * @return Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withBrokerId()   {
        printSectionHeader("Broker ID");
        writer.println("broker.id=${STRIMZI_BROKER_ID}");

        writer.println();

        return this;
    }

    /**
     * Configures the Cruise Control metric reporter. It is set only if user enabled the Cruise Control.
     *
     * @param clusterName Name of the cluster
     * @param cruiseControl The Cruise Control configuration from the Kafka CR
     * @param numPartitions The number of partitions specified in the Kafka config
     * @param replicationFactor The replication factor specified in the Kafka config
     *
     * @return Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withCruiseControl(String clusterName, CruiseControlSpec cruiseControl, String numPartitions, String replicationFactor)   {
        if (cruiseControl != null) {
            printSectionHeader("Cruise Control configuration");
            writer.println("cruise.control.metrics.topic=strimzi.cruisecontrol.metrics");
            writer.println("cruise.control.metrics.reporter.ssl.endpoint.identification.algorithm=HTTPS");
            // using the brokers service because the Admin client, in the Cruise Control metrics reporter, is not able to connect
            // to the pods behind the bootstrap one when they are not ready during startup.
            writer.println("cruise.control.metrics.reporter.bootstrap.servers=" + KafkaResources.brokersServiceName(clusterName) + ":9091");
            writer.println("cruise.control.metrics.reporter.security.protocol=SSL");
            writer.println("cruise.control.metrics.reporter.ssl.keystore.type=PKCS12");
            writer.println("cruise.control.metrics.reporter.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
            writer.println("cruise.control.metrics.reporter.ssl.keystore.password=${CERTS_STORE_PASSWORD}");
            writer.println("cruise.control.metrics.reporter.ssl.truststore.type=PKCS12");
            writer.println("cruise.control.metrics.reporter.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
            writer.println("cruise.control.metrics.reporter.ssl.truststore.password=${CERTS_STORE_PASSWORD}");
            writer.println("cruise.control.metrics.topic.auto.create=true");
            writer.println("cruise.control.metrics.reporter.kubernetes.mode=true");
            if (numPartitions != null) {
                writer.println("cruise.control.metrics.topic.num.partitions=" + numPartitions);
            }
            if (replicationFactor != null) {
                writer.println("cruise.control.metrics.topic.replication.factor=" + replicationFactor);
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
            writer.println("broker.rack=${STRIMZI_RACK_ID}");
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
        writer.println(String.format("zookeeper.connect=%s:%d", ZookeeperCluster.serviceName(clusterName), ZookeeperCluster.CLIENT_TLS_PORT));
        writer.println("zookeeper.clientCnxnSocket=org.apache.zookeeper.ClientCnxnSocketNetty");
        writer.println("zookeeper.ssl.client.enable=true");
        writer.println("zookeeper.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
        writer.println("zookeeper.ssl.keystore.password=${CERTS_STORE_PASSWORD}");
        writer.println("zookeeper.ssl.keystore.type=PKCS12");
        writer.println("zookeeper.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
        writer.println("zookeeper.ssl.truststore.password=${CERTS_STORE_PASSWORD}");
        writer.println("zookeeper.ssl.truststore.type=PKCS12");
        writer.println();

        return this;
    }

    /**
     * Configures the listeners based on the listeners enabled by the users in the Kafka CR.
     *
     * @param clusterName     Name of the cluster (important for the advertised hostnames)
     * @param namespace       Namespace (important for generating the advertised hostname)
     * @param kafkaListeners  The listeners configuration from the Kafka CR
     *
     * @return  Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withListeners(String clusterName, String namespace, KafkaListeners kafkaListeners)  {
        List<String> listeners = new ArrayList<>();
        List<String> advertisedListeners = new ArrayList<>();
        List<String> securityProtocol = new ArrayList<>();

        // Replication listener
        listeners.add("REPLICATION-9091://0.0.0.0:9091");
        advertisedListeners.add(String.format("REPLICATION-9091://%s:9091",
                ModelUtils.podDnsNameWithoutClusterDomain(namespace,
                        KafkaResources.brokersServiceName(clusterName),
                        // Pod name constructed to be templatable for each individual ordinal
                        KafkaResources.kafkaStatefulSetName(clusterName) + "-${STRIMZI_BROKER_ID}")
        ));
        securityProtocol.add("REPLICATION-9091:SSL");
        configureReplicationListener();

        if (kafkaListeners != null) {
            // PLAIN listener
            if (kafkaListeners.getPlain() != null) {
                printSectionHeader("Plain listener");

                int port = 9092;
                String listenerName = "PLAIN-" + port;
                listeners.add(listenerName + "://0.0.0.0:" + port);
                advertisedListeners.add(getAdvertisedListener(clusterName, namespace, listenerName, port));
                configureAuthentication(listenerName, securityProtocol, false, kafkaListeners.getPlain().getAuth());

                writer.println();
            }

            if (kafkaListeners.getTls() != null) {
                printSectionHeader("TLS listener");

                int port = 9093;
                String listenerName = "TLS-" + port;
                listeners.add(listenerName + "://0.0.0.0:" + port);
                advertisedListeners.add(getAdvertisedListener(clusterName, namespace, listenerName, port));
                configureAuthentication(listenerName, securityProtocol, true, kafkaListeners.getTls().getAuth());

                CertAndKeySecretSource customServerCert = null;
                if (kafkaListeners.getTls().getConfiguration() != null) {
                    customServerCert = kafkaListeners.getTls().getConfiguration().getBrokerCertChainAndKey();
                }

                configureTls(listenerName, customServerCert);
            }

            // External listener
            if (kafkaListeners.getExternal() != null) {
                printSectionHeader("External listener");

                String listenerName = "EXTERNAL-9094";
                listeners.add(listenerName + "://0.0.0.0:9094");
                advertisedListeners.add(String.format("%s://${STRIMZI_EXTERNAL_9094_ADVERTISED_HOSTNAME}:${STRIMZI_EXTERNAL_9094_ADVERTISED_PORT}", listenerName));

                KafkaListenerExternal external = kafkaListeners.getExternal();

                if (external instanceof KafkaListenerExternalIngress) {
                    KafkaListenerExternalIngress ingress = (KafkaListenerExternalIngress) external;

                    configureAuthentication(listenerName, securityProtocol, true, kafkaListeners.getExternal().getAuth());

                    CertAndKeySecretSource customServerCert = null;
                    if (ingress.getConfiguration() != null) {
                        customServerCert = ingress.getConfiguration().getBrokerCertChainAndKey();
                    }

                    configureTls(listenerName, customServerCert);
                } else if (external instanceof KafkaListenerExternalNodePort)   {
                    KafkaListenerExternalNodePort nodePort = (KafkaListenerExternalNodePort) external;

                    configureAuthentication(listenerName, securityProtocol, nodePort.isTls(), kafkaListeners.getExternal().getAuth());

                    if (nodePort.isTls())   {
                        CertAndKeySecretSource customServerCert = null;
                        if (nodePort.getConfiguration() != null) {
                            customServerCert = nodePort.getConfiguration().getBrokerCertChainAndKey();
                        }

                        configureTls(listenerName, customServerCert);
                    }
                } else if (external instanceof KafkaListenerExternalRoute)  {
                    KafkaListenerExternalRoute route = (KafkaListenerExternalRoute) external;

                    configureAuthentication(listenerName, securityProtocol, true, kafkaListeners.getExternal().getAuth());

                    CertAndKeySecretSource customServerCert = null;
                    if (route.getConfiguration() != null) {
                        customServerCert = route.getConfiguration().getBrokerCertChainAndKey();
                    }

                    configureTls(listenerName, customServerCert);
                } else if (external instanceof KafkaListenerExternalLoadBalancer)   {
                    KafkaListenerExternalLoadBalancer loadbalancer = (KafkaListenerExternalLoadBalancer) external;

                    configureAuthentication(listenerName, securityProtocol, loadbalancer.isTls(), kafkaListeners.getExternal().getAuth());

                    if (loadbalancer.isTls())   {
                        CertAndKeySecretSource customServerCert = null;
                        if (loadbalancer.getConfiguration() != null) {
                            customServerCert = loadbalancer.getConfiguration().getBrokerCertChainAndKey();
                        }

                        configureTls(listenerName, customServerCert);
                    }
                }
            }
        }

        printSectionHeader("Common listener configuration");
        writer.println("listeners=" + String.join(",", listeners));
        writer.println("advertised.listeners=" + String.join(",", advertisedListeners));
        writer.println("listener.security.protocol.map=" + String.join(",", securityProtocol));
        writer.println("inter.broker.listener.name=REPLICATION-9091");
        writer.println("sasl.enabled.mechanisms=");
        writer.println("ssl.secure.random.implementation=SHA1PRNG");
        writer.println("ssl.endpoint.identification.algorithm=HTTPS");
        writer.println();

        return this;
    }

    /**
     * Internal method which configures the replication listener. The replication listener configuration is currently
     * rather static, it always uses TLS with TLS client auth.
     */
    private void configureReplicationListener() {
        printSectionHeader("Replication listener");
        writer.println("listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
        writer.println("listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}");
        writer.println("listener.name.replication-9091.ssl.keystore.type=PKCS12");
        writer.println("listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
        writer.println("listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}");
        writer.println("listener.name.replication-9091.ssl.truststore.type=PKCS12");
        writer.println("listener.name.replication-9091.ssl.client.auth=required");
        writer.println();
    }

    /**
     * Internal method for generating the advertised listener string for the internal interfaces.
     *
     * @param clusterName   Name of the Kafka STS
     * @param namespace Namespace where the lcuster is deployed
     * @param listenerName  Name of the listener in the Kafka broker configuration
     * @param port  Port on which is this listener listening
     *
     * @return  String with advertised listener configuration
     */
    private String getAdvertisedListener(String clusterName, String namespace, String listenerName, int port)    {
        return String.format("%s://%s:%d",
                listenerName,
                ModelUtils.podDnsNameWithoutClusterDomain(namespace,
                        KafkaResources.brokersServiceName(clusterName),
                        // Pod name constructed to be templatable for each individual ordinal
                        KafkaResources.kafkaStatefulSetName(clusterName) + "-${STRIMZI_BROKER_ID}"),
                port);
    }

    /**
     * Configures TLS for a specific listener. This method is used only internally.
     *
     * @param listenerName  The name of the listener under which it is used in the KAfka broker configuration file
     * @param serverCertificate The custom certificate configuration (null if not specified by the user in the Kafka CR)
     */
    private void configureTls(String listenerName, CertAndKeySecretSource serverCertificate) {
        String listenerNameInProperty = listenerName.toLowerCase(Locale.ENGLISH);

        if (serverCertificate != null)  {
            writer.println(String.format("listener.name.%s.ssl.keystore.location=/tmp/kafka/custom-%s.keystore.p12", listenerNameInProperty, listenerNameInProperty));
        } else {
            writer.println(String.format("listener.name.%s.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12", listenerNameInProperty));
        }

        writer.println(String.format("listener.name.%s.ssl.keystore.password=${CERTS_STORE_PASSWORD}", listenerNameInProperty));
        writer.println(String.format("listener.name.%s.ssl.keystore.type=PKCS12", listenerNameInProperty));

        writer.println();
    }

    /**
     * Configures authentication for a Kafka listener. This method is used only internally.
     *
     * @param listenerName  Name of the listener as used in the Kafka broker configuration file.
     * @param securityProtocol  List of security protocols enabled int he broker. The method will add the security
     *                          protocol configuration for this listener to this list (e.g. SASL_PLAINTEXT).
     * @param tls   Flag whether this protocol is using TLS or not
     * @param auth  The authentication confgiuration from the Kafka CR
     */
    private void configureAuthentication(String listenerName, List<String> securityProtocol, boolean tls, KafkaListenerAuthentication auth)    {
        String listenerNameInProperty = listenerName.toLowerCase(Locale.ENGLISH);
        String listenerNameInEnvVar = listenerName.replace("-", "_");

        if (auth instanceof KafkaListenerAuthenticationOAuth) {
            securityProtocol.add(String.format("%s:%s", listenerName, getSecurityProtocol(tls, true)));

            KafkaListenerAuthenticationOAuth oauth = (KafkaListenerAuthenticationOAuth) auth;
            List<String> options = new ArrayList<>();
            options.addAll(getOAuthOptions(oauth));

            if (oauth.getClientSecret() != null)    {
                options.add("oauth.client.secret=\"${STRIMZI_" + listenerNameInEnvVar + "_OAUTH_CLIENT_SECRET}\"");
            }

            if (oauth.getTlsTrustedCertificates() != null && oauth.getTlsTrustedCertificates().size() > 0)    {
                options.add(String.format("oauth.ssl.truststore.location=\"/tmp/kafka/oauth-%s.truststore.p12\"", listenerNameInProperty));
                options.add("oauth.ssl.truststore.password=\"${CERTS_STORE_PASSWORD}\"");
                options.add("oauth.ssl.truststore.type=\"PKCS12\"");
            }

            writer.println(String.format("listener.name.%s.oauthbearer.sasl.server.callback.handler.class=io.strimzi.kafka.oauth.server.JaasServerOauthValidatorCallbackHandler", listenerNameInProperty));
            writer.println(String.format("listener.name.%s.oauthbearer.sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required unsecuredLoginStringClaim_sub=\"thePrincipalName\" %s;", listenerNameInProperty, String.join(" ", options)));
            writer.println(String.format("listener.name.%s.sasl.enabled.mechanisms=OAUTHBEARER", listenerNameInProperty));

            if (oauth.getMaxSecondsWithoutReauthentication() != null) {
                writer.println(String.format("listener.name.%s.connections.max.reauth.ms=%s", listenerNameInProperty, 1000 * oauth.getMaxSecondsWithoutReauthentication()));
            }

            writer.println();
        } else if (auth instanceof KafkaListenerAuthenticationScramSha512) {
            securityProtocol.add(String.format("%s:%s", listenerName, getSecurityProtocol(tls, true)));

            writer.println(String.format("listener.name.%s.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;", listenerNameInProperty));
            writer.println(String.format("listener.name.%s.sasl.enabled.mechanisms=SCRAM-SHA-512", listenerNameInProperty));
            writer.println();
        } else if (auth instanceof KafkaListenerAuthenticationTls) {
            securityProtocol.add(String.format("%s:%s", listenerName, getSecurityProtocol(tls, false)));

            writer.println(String.format("listener.name.%s.ssl.client.auth=required", listenerNameInProperty));
            writer.println(String.format("listener.name.%s.ssl.truststore.location=/tmp/kafka/clients.truststore.p12", listenerNameInProperty));
            writer.println(String.format("listener.name.%s.ssl.truststore.password=${CERTS_STORE_PASSWORD}", listenerNameInProperty));
            writer.println(String.format("listener.name.%s.ssl.truststore.type=PKCS12", listenerNameInProperty));
            writer.println();
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
    /*test*/ static List<String> getOAuthOptions(KafkaListenerAuthenticationOAuth oauth)  {
        List<String> options = new ArrayList<>(5);

        addOption(options, ServerConfig.OAUTH_CLIENT_ID, oauth.getClientId());
        addOption(options, ServerConfig.OAUTH_VALID_ISSUER_URI, oauth.getValidIssuerUri());
        addBooleanOptionIfFalse(options, ServerConfig.OAUTH_CHECK_ISSUER, oauth.isCheckIssuer());
        addOption(options, ServerConfig.OAUTH_JWKS_ENDPOINT_URI, oauth.getJwksEndpointUri());
        if (oauth.getJwksRefreshSeconds() != null && oauth.getJwksRefreshSeconds() > 0) {
            addOption(options, ServerConfig.OAUTH_JWKS_REFRESH_SECONDS, String.valueOf(oauth.getJwksRefreshSeconds()));
        }
        if (oauth.getJwksRefreshSeconds() != null && oauth.getJwksExpirySeconds() > 0) {
            addOption(options, ServerConfig.OAUTH_JWKS_EXPIRY_SECONDS, String.valueOf(oauth.getJwksExpirySeconds()));
        }
        if (oauth.getJwksMinRefreshPauseSeconds() != null && oauth.getJwksMinRefreshPauseSeconds() >= 0) {
            addOption(options, ServerConfig.OAUTH_JWKS_REFRESH_MIN_PAUSE_SECONDS, String.valueOf(oauth.getJwksMinRefreshPauseSeconds()));
        }
        addBooleanOptionIfTrue(options, ServerConfig.OAUTH_CRYPTO_PROVIDER_BOUNCYCASTLE, oauth.isEnableECDSA());
        addOption(options, ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, oauth.getIntrospectionEndpointUri());
        addOption(options, ServerConfig.OAUTH_USERINFO_ENDPOINT_URI, oauth.getUserInfoEndpointUri());
        addOption(options, ServerConfig.OAUTH_USERNAME_CLAIM, oauth.getUserNameClaim());
        addOption(options, ServerConfig.OAUTH_FALLBACK_USERNAME_CLAIM, oauth.getFallbackUserNameClaim());
        addOption(options, ServerConfig.OAUTH_FALLBACK_USERNAME_PREFIX, oauth.getFallbackUserNamePrefix());
        addBooleanOptionIfFalse(options, ServerConfig.OAUTH_ACCESS_TOKEN_IS_JWT, oauth.isAccessTokenIsJwt());
        addBooleanOptionIfFalse(options, ServerConfig.OAUTH_CHECK_ACCESS_TOKEN_TYPE, oauth.isCheckAccessTokenType());
        addOption(options, ServerConfig.OAUTH_VALID_TOKEN_TYPE, oauth.getValidTokenType());

        if (oauth.isDisableTlsHostnameVerification()) {
            addOption(options, ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, "");
        }

        return options;
    }

    static void addOption(List<String> options, String option, String value) {
        if (value != null) options.add(String.format("%s=\"%s\"", option, value));
    }

    static void addBooleanOptionIfTrue(List<String> options, String option, boolean value) {
        if (value) options.add(String.format("%s=\"%s\"", option, value));
    }

    static void addBooleanOptionIfFalse(List<String> options, String option, boolean value) {
        if (!value) options.add(String.format("%s=\"%s\"", option, value));
    }

    static void addOption(PrintWriter writer, String name, Object value) {
        if (value != null) writer.println(name + "=" + value);
    }

    /**
     * Configures authorization for the Kafka cluster.
     *
     * @param clusterName   The name of the cluster (used to configure the replication super users)
     * @param authorization The authorization configuration from the Kafka CR
     *
     * @return  Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withAuthorization(String clusterName, KafkaAuthorization authorization)  {
        if (authorization != null) {
            List<String> superUsers = new ArrayList<>();

            // Broker super users
            superUsers.add(String.format("User:CN=%s,O=io.strimzi", KafkaResources.kafkaStatefulSetName(clusterName)));
            superUsers.add(String.format("User:CN=%s-%s,O=io.strimzi", clusterName, "entity-operator"));
            superUsers.add(String.format("User:CN=%s-%s,O=io.strimzi", clusterName, "kafka-exporter"));
            superUsers.add(String.format("User:CN=%s-%s,O=io.strimzi", clusterName, "cruise-control"));

            superUsers.add(String.format("User:CN=%s,O=io.strimzi", "cluster-operator"));

            printSectionHeader("Authorization");
            configureAuthorization(clusterName, superUsers, authorization);
            writer.println("super.users=" + String.join(";", superUsers));
            writer.println();
        }

        return this;
    }

    /**
     * Configures authorization for the Kafka brokers. This method is used only internally.
     *
     * @param clusterName Name of the cluster
     * @param superUsers Super users list who have all the rights on the cluster
     * @param authorization The authorization configuration from the Kafka CR
     */
    private void configureAuthorization(String clusterName, List<String> superUsers, KafkaAuthorization authorization) {
        if (KafkaAuthorizationSimple.TYPE_SIMPLE.equals(authorization.getType())) {
            KafkaAuthorizationSimple simpleAuthz = (KafkaAuthorizationSimple) authorization;
            writer.println("authorizer.class.name=" + KafkaAuthorizationSimple.AUTHORIZER_CLASS_NAME);

            // User configured super users
            if (simpleAuthz.getSuperUsers() != null && simpleAuthz.getSuperUsers().size() > 0) {
                superUsers.addAll(simpleAuthz.getSuperUsers().stream().map(e -> String.format("User:%s", e)).collect(Collectors.toList()));
            }
        } else if (KafkaAuthorizationOpa.TYPE_OPA.equals(authorization.getType())) {
            KafkaAuthorizationOpa opaAuthz = (KafkaAuthorizationOpa) authorization;
            writer.println("authorizer.class.name=" + KafkaAuthorizationOpa.AUTHORIZER_CLASS_NAME);

            writer.println(String.format("%s=%s", "opa.authorizer.url", opaAuthz.getUrl()));
            writer.println(String.format("%s=%b", "opa.authorizer.allow.on.error", opaAuthz.isAllowOnError()));
            writer.println(String.format("%s=%d", "opa.authorizer.cache.initial.capacity", opaAuthz.getInitialCacheCapacity()));
            writer.println(String.format("%s=%d", "opa.authorizer.cache.maximum.size", opaAuthz.getMaximumCacheSize()));
            writer.println(String.format("%s=%d", "opa.authorizer.cache.expire.after.seconds", Duration.ofMillis(opaAuthz.getExpireAfterMs()).getSeconds()));

            // User configured super users
            if (opaAuthz.getSuperUsers() != null && opaAuthz.getSuperUsers().size() > 0) {
                superUsers.addAll(opaAuthz.getSuperUsers().stream().map(e -> String.format("User:%s", e)).collect(Collectors.toList()));
            }
        } else if (KafkaAuthorizationKeycloak.TYPE_KEYCLOAK.equals(authorization.getType())) {
            KafkaAuthorizationKeycloak keycloakAuthz = (KafkaAuthorizationKeycloak) authorization;
            writer.println("authorizer.class.name=" + KafkaAuthorizationKeycloak.AUTHORIZER_CLASS_NAME);
            writer.println("principal.builder.class=" + KafkaAuthorizationKeycloak.PRINCIPAL_BUILDER_CLASS_NAME);
            writer.println("strimzi.authorization.token.endpoint.uri=" + keycloakAuthz.getTokenEndpointUri());
            writer.println("strimzi.authorization.client.id=" + keycloakAuthz.getClientId());
            writer.println("strimzi.authorization.delegate.to.kafka.acl=" + keycloakAuthz.isDelegateToKafkaAcls());
            addOption(writer, "strimzi.authorization.grants.refresh.period.seconds", keycloakAuthz.getGrantsRefreshPeriodSeconds());
            addOption(writer, "strimzi.authorization.grants.refresh.pool.size", keycloakAuthz.getGrantsRefreshPoolSize());
            writer.println("strimzi.authorization.kafka.cluster.name=" + clusterName);

            if (keycloakAuthz.getTlsTrustedCertificates() != null && keycloakAuthz.getTlsTrustedCertificates().size() > 0)    {
                writer.println("strimzi.authorization.ssl.truststore.location=/tmp/kafka/authz-keycloak.truststore.p12");
                writer.println("strimzi.authorization.ssl.truststore.password=${CERTS_STORE_PASSWORD}");
                writer.println("strimzi.authorization.ssl.truststore.type=PKCS12");
                writer.println("strimzi.authorization.ssl.secure.random.implementation=SHA1PRNG");
                String endpointIdentificationAlgorithm = keycloakAuthz.isDisableTlsHostnameVerification() ? "" : "HTTPS";
                writer.println("strimzi.authorization.ssl.endpoint.identification.algorithm=" + endpointIdentificationAlgorithm);
            }

            // User configured super users
            if (keycloakAuthz.getSuperUsers() != null && keycloakAuthz.getSuperUsers().size() > 0) {
                superUsers.addAll(keycloakAuthz.getSuperUsers().stream().map(e -> String.format("User:%s", e)).collect(Collectors.toList()));
            }
        }
    }

    /**
     * Configures the configuration options passed by the user in the Kafka CR.
     *
     * @param userConfig    The User configuration - Kafka broker configuration options specified by the user in the Kafka custom resource
     *
     * @return  Returns the builder instance
     */
    public KafkaBrokerConfigurationBuilder withUserConfiguration(AbstractConfiguration userConfig)  {
        if (userConfig != null && !userConfig.getConfiguration().isEmpty()) {
            printSectionHeader("User provided configuration");
            writer.println(userConfig.getConfiguration());
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
                .map(volumeMount -> volumeMount.getMountPath() + "/kafka-log${STRIMZI_BROKER_ID}").collect(Collectors.joining(","));

        printSectionHeader("Kafka message logs configuration");
        writer.println("log.dirs=" + logDirs);
        writer.println();

        return this;
    }

    /**
     * Internal method which prints the section header into the configuration file. This makes it more human readable
     * when looking for issues in runnign pods etc.
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
