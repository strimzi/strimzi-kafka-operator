/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.dynamicconfig;

import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.KafkaAuthorization;
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
import io.strimzi.operator.cluster.model.AbstractConfiguration;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class KafkaBrokerConfigurationBuilder {
    private final StringWriter stringWriter = new StringWriter();
    private final PrintWriter writer = new PrintWriter(stringWriter);

    public KafkaBrokerConfigurationBuilder() {
        printHeader();
    }

    public KafkaBrokerConfigurationBuilder withBrokerId()   {
        printSectionHeader("Broker ID");
        writer.println("broker.id=${STRIMZI_BROKER_ID}");
        writer.println();

        return this;
    }

    public KafkaBrokerConfigurationBuilder withRackId(Rack rack)   {
        if (rack != null) {
            printSectionHeader("Rack ID");
            writer.println("rack.id=${STRIMZI_RACK_ID}");
            writer.println();
        }

        return this;
    }

    public KafkaBrokerConfigurationBuilder withZookeeper()  {
        printSectionHeader("Zookeeper");
        writer.println("zookeeper.connect=localhost:2181");
        writer.println();

        return this;
    }

    public KafkaBrokerConfigurationBuilder withListeners(String clusterName, String namespace, KafkaListeners kafkaListeners)  {
        List<String> listeners = new ArrayList<>();
        List<String> advertisedListeners = new ArrayList<>();
        List<String> securityProtocol = new ArrayList<>();

        // Replication listener
        listeners.add("REPLICATION-9091://0.0.0.0:9091");
        advertisedListeners.add(String.format("REPLICATION-9091://%s-${STRIMZI_BROKER_ID}.%s-brokers.%s.svc:9091", KafkaResources.kafkaStatefulSetName(clusterName), KafkaResources.kafkaStatefulSetName(clusterName), namespace));
        securityProtocol.add("REPLICATION-9091:SSL");

        printSectionHeader("Replication listener");
        writer.println("listener.name.replication-9091.ssl.keystore.location=/tmp/kafka/cluster.keystore.p12");
        writer.println("listener.name.replication-9091.ssl.keystore.password=${CERTS_STORE_PASSWORD}");
        writer.println("listener.name.replication-9091.ssl.keystore.type=PKCS12");
        writer.println("listener.name.replication-9091.ssl.truststore.location=/tmp/kafka/cluster.truststore.p12");
        writer.println("listener.name.replication-9091.ssl.truststore.password=${CERTS_STORE_PASSWORD}");
        writer.println("listener.name.replication-9091.ssl.truststore.type=PKCS12");
        writer.println("listener.name.replication-9091.ssl.client.auth=required");
        writer.println();

        if (kafkaListeners != null) {
            // PLAIN listener
            if (kafkaListeners.getPlain() != null) {
                printSectionHeader("Plain listener");

                String listenerName = "PLAIN-9092";
                listeners.add(listenerName + "://0.0.0.0:9092");
                advertisedListeners.add(String.format("%s://%s-${STRIMZI_BROKER_ID}.%s-brokers.%s.svc:9092", listenerName, KafkaResources.kafkaStatefulSetName(clusterName), KafkaResources.kafkaStatefulSetName(clusterName), namespace));
                configureAuthentication(writer, listenerName, securityProtocol, false, kafkaListeners.getPlain().getAuth());

                writer.println();
            }

            if (kafkaListeners.getTls() != null) {
                printSectionHeader("TLS listener");

                String listenerName = "TLS-9093";
                listeners.add(listenerName + "://0.0.0.0:9093");
                advertisedListeners.add(String.format("%s://%s-${STRIMZI_BROKER_ID}.%s-brokers.%s.svc:9093", listenerName, KafkaResources.kafkaStatefulSetName(clusterName), KafkaResources.kafkaStatefulSetName(clusterName), namespace));
                configureAuthentication(writer, listenerName, securityProtocol, true, kafkaListeners.getTls().getAuth());

                CertAndKeySecretSource customServerCert = null;
                if (kafkaListeners.getTls().getConfiguration() != null) {
                    customServerCert = kafkaListeners.getTls().getConfiguration().getBrokerCertChainAndKey();
                }

                configureTls(writer, listenerName, customServerCert);
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

                    configureAuthentication(writer, listenerName, securityProtocol, true, kafkaListeners.getExternal().getAuth());

                    CertAndKeySecretSource customServerCert = null;
                    if (ingress.getConfiguration() != null) {
                        customServerCert = ingress.getConfiguration().getBrokerCertChainAndKey();
                    }

                    configureTls(writer, listenerName, customServerCert);
                } else if (external instanceof KafkaListenerExternalNodePort)   {
                    KafkaListenerExternalNodePort nodePort = (KafkaListenerExternalNodePort) external;

                    configureAuthentication(writer, listenerName, securityProtocol, nodePort.isTls(), kafkaListeners.getExternal().getAuth());

                    if (nodePort.isTls())   {
                        CertAndKeySecretSource customServerCert = null;
                        if (nodePort.getConfiguration() != null) {
                            customServerCert = nodePort.getConfiguration().getBrokerCertChainAndKey();
                        }

                        configureTls(writer, listenerName, customServerCert);
                    }
                } else if (external instanceof KafkaListenerExternalRoute)  {
                    KafkaListenerExternalRoute route = (KafkaListenerExternalRoute) external;

                    configureAuthentication(writer, listenerName, securityProtocol, true, kafkaListeners.getExternal().getAuth());

                    CertAndKeySecretSource customServerCert = null;
                    if (route.getConfiguration() != null) {
                        customServerCert = route.getConfiguration().getBrokerCertChainAndKey();
                    }

                    configureTls(writer, listenerName, customServerCert);
                } else if (external instanceof KafkaListenerExternalLoadBalancer)   {
                    KafkaListenerExternalLoadBalancer loadbalancer = (KafkaListenerExternalLoadBalancer) external;

                    configureAuthentication(writer, listenerName, securityProtocol, loadbalancer.isTls(), kafkaListeners.getExternal().getAuth());

                    if (loadbalancer.isTls())   {
                        CertAndKeySecretSource customServerCert = null;
                        if (loadbalancer.getConfiguration() != null) {
                            customServerCert = loadbalancer.getConfiguration().getBrokerCertChainAndKey();
                        }

                        configureTls(writer, listenerName, customServerCert);
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

    private void configureTls(PrintWriter writer, String listenerName, CertAndKeySecretSource serverCertificate) {
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

    private void configureAuthentication(PrintWriter writer, String listenerName, List<String> securityProtocol, boolean tls, KafkaListenerAuthentication auth)    {
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

    private String getSecurityProtocol(boolean tls, boolean sasl)   {
        if (tls)    {
            if (sasl)   {
                return "SASL_SSL";
            } else {
                return "SSL";
            }
        } else  {
            if (sasl)   {
                return "SASL_PLAINTEXT";
            } else {
                return "PLAINTEXT";
            }
        }
    }

    /**
     * Generates the public part of the OAUTH configuration for JAAS. The private part is not added here but as a secret
     * reference to keep it secure.
     *
     * @param oauth     OAuth type authentication object
     * @return  JAAS configuration options with the public variables
     */
    /*test*/ static List<String> getOAuthOptions(KafkaListenerAuthenticationOAuth oauth)  {
        List<String> options = new ArrayList<>(5);

        if (oauth.getClientId() != null) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_CLIENT_ID, oauth.getClientId()));
        if (oauth.getValidIssuerUri() != null) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_VALID_ISSUER_URI, oauth.getValidIssuerUri()));
        if (oauth.getJwksEndpointUri() != null) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_JWKS_ENDPOINT_URI, oauth.getJwksEndpointUri()));
        if (oauth.getJwksRefreshSeconds() > 0) options.add(String.format("%s=\"%d\"", ServerConfig.OAUTH_JWKS_REFRESH_SECONDS, oauth.getJwksRefreshSeconds()));
        if (oauth.getJwksExpirySeconds() > 0) options.add(String.format("%s=\"%d\"", ServerConfig.OAUTH_JWKS_EXPIRY_SECONDS, oauth.getJwksExpirySeconds()));
        if (oauth.getIntrospectionEndpointUri() != null) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_INTROSPECTION_ENDPOINT_URI, oauth.getIntrospectionEndpointUri()));
        if (oauth.getUserNameClaim() != null) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_USERNAME_CLAIM, oauth.getUserNameClaim()));
        if (!oauth.isAccessTokenIsJwt()) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_TOKENS_NOT_JWT, true));
        if (!oauth.isCheckAccessTokenType()) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_VALIDATION_SKIP_TYPE_CHECK, true));
        if (oauth.isDisableTlsHostnameVerification()) options.add(String.format("%s=\"%s\"", ServerConfig.OAUTH_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM, ""));

        return options;
    }

    public KafkaBrokerConfigurationBuilder withAuthorization(String clusterName, KafkaAuthorization authorization)  {
        if (authorization != null) {
            List<String> superUsers = new ArrayList<>();
            String authorizerClass = "";

            // Broker super users
            superUsers.add(String.format("User:CN=%s,O=io.strimzi", KafkaResources.kafkaStatefulSetName(clusterName)));
            superUsers.add(String.format("User:CN=%s-%s,O=io.strimzi", clusterName, "entity-operator"));
            superUsers.add(String.format("User:CN=%s-%s,O=io.strimzi", clusterName, "kafka-exporter"));

            // User configured super users
            if (KafkaAuthorizationSimple.TYPE_SIMPLE.equals(authorization.getType())) {
                KafkaAuthorizationSimple simpleAuthz = (KafkaAuthorizationSimple) authorization;
                authorizerClass = KafkaAuthorizationSimple.AUTHORIZER_CLASS_NAME;

                if (simpleAuthz.getSuperUsers() != null && simpleAuthz.getSuperUsers().size() > 0) {
                    superUsers.addAll(simpleAuthz.getSuperUsers().stream().map(e -> String.format("User:%s", e)).collect(Collectors.toList()));
                }
            }

            printSectionHeader("Authorization");
            writer.println("authorizer.class.name=" + authorizerClass);
            writer.println("super.users=" + String.join(";", superUsers));
            writer.println();
        }

        return this;
    }

    public KafkaBrokerConfigurationBuilder withUserConfiguration(AbstractConfiguration userConfig)  {
        if (userConfig != null && !userConfig.getConfiguration().isEmpty()) {
            printSectionHeader("User provided configuration");
            writer.println(userConfig.getConfiguration());
            writer.println();
        }

        return this;
    }

    public KafkaBrokerConfigurationBuilder withLogDirs(List<VolumeMount> mounts)  {
        // We take all the data mount points and add the broker specific path
        String logDirs = mounts.stream()
                .map(volumeMount -> volumeMount.getMountPath() + "/kafka-log${STRIMZI_BROKER_ID}").collect(Collectors.joining(","));

        printSectionHeader("Kafka message logs configuration");
        writer.println("log.dirs=" + logDirs);
        writer.println();

        return this;
    }

    private void printSectionHeader(String sectionName)   {
        writer.println("##########");
        writer.println("# " + sectionName);
        writer.println("##########");
    }

    private void printHeader()   {
        writer.println("##############################");
        writer.println("##############################");
        writer.println("# This file is automatically generated by the Strimzi Cluster Operator");
        writer.println("# Any changes to this file will be ignored and overwritten!");
        writer.println("##############################");
        writer.println("##############################");
        writer.println();
    }

    public String build()  {
        return stringWriter.toString();
    }
}
