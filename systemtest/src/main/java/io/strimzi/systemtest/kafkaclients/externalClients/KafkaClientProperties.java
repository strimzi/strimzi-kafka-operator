/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.kafka.oauth.common.HttpUtil;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.EClientType;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.Iterator;
import java.util.Properties;

import static io.strimzi.api.kafka.model.KafkaResources.externalBootstrapServiceName;
import static io.strimzi.kafka.oauth.common.OAuthAuthenticator.urlencode;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assertions.fail;

@SuppressFBWarnings("REC_CATCH_EXCEPTION")
class KafkaClientProperties {

    private static final Logger LOGGER = LogManager.getLogger(KafkaClientProperties.class);

    /**
     * Create producer properties with PLAINTEXT security
     *
     * @param namespace   kafka namespace
     * @param clusterName kafka cluster name
     * @return producer properties
     */
    static Properties createBasicProducerProperties(String namespace, String clusterName) throws IOException {
        return createProducerProperties(namespace, clusterName, EClientType.BASIC);
    }

    /**
     * Create producer properties with secure communication security
     *
     * @param namespace        kafka namespace
     * @param clusterName      kafka cluster name
     * @param caCertName       custom or ca certificate to use for secure communication
     * @param kafkaUsername    kafka username
     * @param securityProtocol security protocol
     * @return producer properties
     */
    static Properties createBasicProducerTlsProperties(String namespace, String clusterName, String caCertName,
                                                       String kafkaUsername, String securityProtocol) throws IOException {
        return createProducerProperties(namespace, clusterName, caCertName, kafkaUsername, securityProtocol);
    }

    /**
     * Create tracing producer properties with PLAINTEXT security
     *
     * @param namespace   kafka namespace
     * @param clusterName kafka cluster name
     * @return producer properties
     */
    static Properties createTracingProducerProperties(String namespace, String clusterName) throws IOException {
        return createProducerProperties(namespace, clusterName, EClientType.TRACING);
    }

    /**
     * Create oauth producer properties with PLAINTEXT security
     *
     * @param namespace             kafka namespace
     * @param clusterName           kafka cluster name
     * @param clientId              oauth client id
     * @param clientSecretName      oauth client secret name
     * @param oauthTokenEndpointUri uri, where client will send a request for token
     * @return producer properties
     */
    static Properties createOauthProducerProperties(String namespace, String clusterName, String clientId,
                                                    String clientSecretName, String oauthTokenEndpointUri) throws IOException {
        return createProducerProperties(namespace, clusterName, EClientType.OAUTH, clientId, clientSecretName, oauthTokenEndpointUri);
    }

    /**
     * Create oauth producer properties with secure communication
     *
     * @param namespace             kafka namespace
     * @param clusterName           kafka cluster name
     * @param caCertName            custom or ca certificate to use for secure communication
     * @param kafkaUsername         kafka username
     * @param securityProtocol      security protocol
     * @param clientId              oauth client id
     * @param clientSecretName      oauth client secret name
     * @param oauthTokenEndpointUri uri, where client will send a request for token
     * @return producer properties
     */
    static Properties createOauthProducerTlsProperties(String namespace, String clusterName, String caCertName, String kafkaUsername,
                                                       String securityProtocol, String clientId,
                                                       String clientSecretName, String oauthTokenEndpointUri) throws IOException {
        return createProducerProperties(namespace, clusterName, caCertName, kafkaUsername, securityProtocol,
                EClientType.OAUTH, clientId, clientSecretName, oauthTokenEndpointUri);
    }

    /**
     * Create producer properties with PLAINTEXT security
     *
     * @param namespace        kafka namespace
     * @param clusterName      kafka cluster name
     * @param caCertName       certificate name
     * @param username         name of the kafka user
     * @param securityProtocol security protocol
     * @return producer properties
     */
    static Properties createProducerProperties(String namespace, String clusterName, String caCertName,
                                               String username, String securityProtocol) throws IOException {
        return createProducerProperties(namespace, clusterName, caCertName, username, securityProtocol, EClientType.BASIC,
                "", "", "");
    }

    /**
     * Create producer properties with PLAINTEXT security
     *
     * @param namespace             kafka namespace
     * @param clusterName           kafka cluster name
     * @param clientId              oauth client id
     * @param clientSecretName      oauth client secret name
     * @param oauthTokenEndpointUri uri, where client will send a request for token
     * @return producer properties
     */
    static Properties createProducerProperties(String namespace, String clusterName, EClientType eClientType, String clientId,
                                               String clientSecretName, String oauthTokenEndpointUri) throws IOException {
        return createProducerProperties(namespace, clusterName, KafkaResources.clusterCaCertificateSecretName(clusterName),
                "", CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL, eClientType,
                clientId, clientSecretName, oauthTokenEndpointUri);
    }

    /**
     * Create producer properties with PLAINTEXT security
     *
     * @param namespace   kafka namespace
     * @param clusterName kafka cluster name
     * @param eClientType enum for specific type of clients
     * @return producer properties
     */
    static Properties createProducerProperties(String namespace, String clusterName, EClientType eClientType) throws IOException {
        return createProducerProperties(namespace, clusterName, KafkaResources.clusterCaCertificateSecretName(clusterName),
                "", CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL, eClientType,
                "", "", "");
    }

    /**
     * Create producer properties with SSL security
     *
     * @param namespace             kafka namespace
     * @param clusterName           kafka cluster name
     * @param caSecretName          CA secret name
     * @param userName              user name for authorization
     * @param securityProtocol      security protocol
     * @param clientType            enum for specific type of clients
     * @param clientId              oauth client id
     * @param clientSecretName      oauth client secret name
     * @param oauthTokenEndpointUri uri, where client will send a request for token
     * @return producer configuration
     */
    static Properties createProducerProperties(String namespace, String clusterName, String caSecretName, String userName,
                                               String securityProtocol, EClientType clientType,
                                               String clientId, String clientSecretName, String oauthTokenEndpointUri) throws IOException {
        Properties producerProperties = new Properties();

        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getExternalBootstrapConnect(namespace, clusterName));
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
        producerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, userName + "-producer");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        producerProperties.putAll(sharedClientProperties(namespace, caSecretName, userName, securityProtocol));

        LOGGER.info("Username has name:{}", userName);

        if (clientType == EClientType.OAUTH) {
            if (userName.equals("")) {
                OauthKafkaClient.setOauthClientPlainProperties(producerProperties, clientId, clientSecretName, oauthTokenEndpointUri);
            } else {
                OauthKafkaClient.setOauthClientTlsProperties(producerProperties, clientId, clientSecretName, oauthTokenEndpointUri);
            }
        }

        return producerProperties;
    }

    /**
     * Create basic consumer properties with plain communication
     *
     * @param namespace     kafka namespace
     * @param clusterName   kafka cluster name
     * @param consumerGroup consumer group
     * @return consumer configuration
     */
    static Properties createBasicConsumerProperties(String namespace, String clusterName, String consumerGroup) throws IOException {
        return createConsumerProperties(namespace, clusterName, consumerGroup, EClientType.BASIC);
    }

    /**
     * Create producer properties with secure communication security
     *
     * @param namespace        kafka namespace
     * @param clusterName      kafka cluster name
     * @param consumerGroup    consumer group name
     * @param caCertName       custom or ca certificate to use for secure communication
     * @param kafkaUsername    kafka username
     * @param securityProtocol security protocol
     * @return producer properties
     */
    static Properties createBasicConsumerTlsProperties(String namespace, String clusterName, String caCertName,
                                                       String kafkaUsername, String securityProtocol, String consumerGroup) throws IOException {
        return createConsumerProperties(namespace, clusterName, caCertName, kafkaUsername, consumerGroup,
                EClientType.BASIC, securityProtocol);
    }

    /**
     * Create tracing producer properties with plain communication
     *
     * @param namespace     kafka namespace
     * @param clusterName   kafka cluster name
     * @param consumerGroup consumer group
     * @return producer properties
     */
    static Properties createTracingConsumerProperties(String namespace, String clusterName, String consumerGroup) throws IOException {
        return createConsumerProperties(namespace, clusterName, consumerGroup, EClientType.TRACING);
    }

    /**
     * Create oauth consumer properties with plain communication
     *
     * @param namespace            kafka namespace
     * @param clusterName          kafka cluster name
     * @param consumerGroup        consumer group name
     * @param clientId             id of oauth client
     * @param clientSecretName     secret of oauth client
     * @param oauthTokenEndpoinUri uri where will client points to get access token
     * @return consumer configuration
     */
    static Properties createOauthConsumerProperties(String namespace, String clusterName, String consumerGroup,
                                                    String clientId, String clientSecretName,
                                                    String oauthTokenEndpoinUri) throws IOException {
        return createConsumerProperties(namespace, clusterName, consumerGroup, EClientType.OAUTH, clientId, clientSecretName,
                oauthTokenEndpoinUri);
    }

    /**
     * Create oauth consumer properties with plain communication
     *
     * @param namespace             kafka namespace
     * @param clusterName           kafka cluster name
     * @param caCertName            custom or ca certificate to use for secure communication
     * @param kafkaUsername         kafka username
     * @param consumerGroup         consumer group
     * @param clientId              id of oauth client
     * @param clientSecretName      secret of oauth client
     * @param oauthTokenEndpointUri uri where will client points to get access token
     * @return consumer configuration
     */
    static Properties createOauthTlsConsumerProperties(String namespace, String clusterName, String caCertName,
                                                       String kafkaUsername, String securityProtocol, String consumerGroup,
                                                       String clientId, String clientSecretName, String oauthTokenEndpointUri) throws IOException {
        return createConsumerProperties(namespace, clusterName, caCertName, kafkaUsername, securityProtocol, consumerGroup,
                EClientType.OAUTH, clientId, clientSecretName, oauthTokenEndpointUri);
    }

    /**
     * Create consumer properties with plain communication
     *
     * @param namespace     kafka namespace
     * @param clusterName   kafka cluster name
     * @param consumerGroup consumer group
     * @param clientType    enum for specific type of clients
     * @return consumer configuration
     */
    static Properties createConsumerProperties(String namespace, String clusterName, String caCertName, String userName,
                                               String consumerGroup, EClientType clientType, String securityProtocol) throws IOException {
        return createConsumerProperties(namespace, clusterName, caCertName, userName, securityProtocol,
                consumerGroup, clientType, "", "", "");
    }


    /**
     * Create consumer properties with plain communication
     *
     * @param namespace             kafka namespace
     * @param clusterName           kafka cluster name
     * @param consumerGroup         consumer group
     * @param clientType            enum for specific type of clients
     * @param clientId              id of oauth client
     * @param clientSecretName      secret of oauth client
     * @param oauthTokenEndpointUri uri where will client points to get access token
     * @return consumer configuration
     */
    static Properties createConsumerProperties(String namespace, String clusterName, String consumerGroup, EClientType clientType,
                                               String clientId, String clientSecretName, String oauthTokenEndpointUri) throws IOException {
        return createConsumerProperties(namespace, clusterName, KafkaResources.clusterCaCertificateSecretName(clusterName),
                "", CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL, consumerGroup, clientType, clientId,
                clientSecretName, oauthTokenEndpointUri);
    }

    /**
     * Create consumer properties with plain communication
     *
     * @param namespace     kafka namespace
     * @param clusterName   kafka cluster name
     * @param consumerGroup consumer group
     * @param clientType    enum for specific type of clients
     * @return consumer configuration
     */
    static Properties createConsumerProperties(String namespace, String clusterName, String consumerGroup, EClientType clientType) throws IOException {
        return createConsumerProperties(namespace, clusterName, KafkaResources.clusterCaCertificateSecretName(clusterName),
                "", CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL, consumerGroup, clientType, "",
                "", "");
    }

    /**
     * Create consumer properties with SSL security
     *
     * @param namespace             kafka namespace
     * @param clusterName           kafka cluster name
     * @param caSecretName          CA secret name
     * @param userName              user name for authorization
     * @param securityProtocol      security protocol
     * @param consumerGroup         consumer group
     * @param clientType            enum for specific type of clients
     * @param clientId              id of oauth client
     * @param clientSecretName      secret of oauth client
     * @param oauthTokenEndpointUri uri where will client points to get access token
     * @return consumer configuration
     */
    static Properties createConsumerProperties(String namespace, String clusterName, String caSecretName, String userName,
                                               String securityProtocol, String consumerGroup, EClientType clientType,
                                               String clientId, String clientSecretName, String oauthTokenEndpointUri) throws IOException {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                getExternalBootstrapConnect(namespace, clusterName));
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, userName + "-consumer");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumerProperties.putAll(sharedClientProperties(namespace, caSecretName, userName, securityProtocol));

        if (clientType == EClientType.OAUTH) {
            if (userName.equals("")) {
                OauthKafkaClient.setOauthClientPlainProperties(consumerProperties, clientId, clientSecretName, oauthTokenEndpointUri);
            } else {
                OauthKafkaClient.setOauthClientTlsProperties(consumerProperties, clientId, clientSecretName, oauthTokenEndpointUri);
            }
        }

        return consumerProperties;
    }

    /**
     * Create properties which are same pro producer and consumer
     *
     * @param namespace        kafka namespace
     * @param caSecretName     CA secret name
     * @param userName         user name for authorization
     * @param securityProtocol security protocol
     * @return shared client properties
     */
    private static Properties sharedClientProperties(String namespace, String caSecretName, String userName, String securityProtocol) {
        Properties properties = new Properties();
        // For turn off hostname verification
        properties.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        try {
            Secret clusterCaCertSecret = kubeClient(namespace).getSecret(caSecretName);
            File tsFile = File.createTempFile(KafkaClientProperties.class.getName(), ".truststore");
            String tsPassword = "foo";
            if (caSecretName.contains("custom-certificate")) {
                tsFile.deleteOnExit();
                KeyStore ts = KeyStore.getInstance(KeyStore.getDefaultType());
                ts.load(null, tsPassword.toCharArray());
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                String clusterCaCert = kubeClient(namespace).getSecret(caSecretName).getData().get("ca.crt");
                Certificate cert = cf.generateCertificate(new ByteArrayInputStream(Base64.getDecoder().decode(clusterCaCert)));
                ts.setCertificateEntry("ca.crt", cert);
                FileOutputStream tsOs = new FileOutputStream(tsFile);
                try {
                    ts.store(tsOs, tsPassword.toCharArray());
                } finally {
                    tsOs.close();
                }
                properties.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, KeyStore.getDefaultType());
            } else {
                tsPassword = new String(Base64.getDecoder().decode(clusterCaCertSecret.getData().get("ca.password")), StandardCharsets.US_ASCII);
                String truststore = clusterCaCertSecret.getData().get("ca.p12");
                Files.write(tsFile.toPath(), Base64.getDecoder().decode(truststore));
                tsFile.deleteOnExit();
            }

            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, tsPassword);
            LOGGER.debug("Truststore password:{}", tsPassword);
            properties.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, tsFile.getAbsolutePath());
            LOGGER.debug("Truststore location {}", tsFile.getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (!userName.isEmpty() && securityProtocol.equals(SecurityProtocol.SASL_SSL.name)) {
            properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
            Secret userSecret = kubeClient(namespace).getSecret(userName);
            String password = new String(Base64.getDecoder().decode(userSecret.getData().get("password")), Charset.forName("UTF-8"));

            String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
            String jaasCfg = String.format(jaasTemplate, userName, password);

            properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasCfg);
        } else if (!userName.isEmpty()) {
            try {

                Secret userSecret = kubeClient(namespace).getSecret(userName);

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
                LOGGER.debug("Keystore location:{}", ksFile.getAbsolutePath());
                properties.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, ksFile.getAbsolutePath());

                properties.setProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return properties;

    }

    /**
     * Get external bootstrap connection
     *
     * @param namespace   kafka namespace
     * @param clusterName kafka cluster name
     * @return bootstrap url as string
     */
    private static String getExternalBootstrapConnect(String namespace, String clusterName) {
        if (kubeClient(namespace).getClient().isAdaptable(OpenShiftClient.class)) {
            Route route = kubeClient(namespace).getClient().adapt(OpenShiftClient.class).routes().inNamespace(namespace).withName(clusterName + "-kafka-bootstrap").get();
            if (route != null && !route.getStatus().getIngress().isEmpty()) {
                return route.getStatus().getIngress().get(0).getHost() + ":443";
            }
        }

        Service extBootstrapService = kubeClient(namespace).getClient().services()
                .inNamespace(namespace)
                .withName(externalBootstrapServiceName(clusterName))
                .get();

        if (extBootstrapService == null) {
            throw new RuntimeException("Kafka cluster " + clusterName + " doesn't have an external bootstrap service");
        }

        String extBootstrapServiceType = extBootstrapService.getSpec().getType();

        if (extBootstrapServiceType.equals("NodePort")) {
            int port = extBootstrapService.getSpec().getPorts().get(0).getNodePort();
            String externalAddress = kubeClient(namespace).listNodes().get(0).getStatus().getAddresses().get(0).getAddress();
            return externalAddress + ":" + port;
        } else if (extBootstrapServiceType.equals("LoadBalancer")) {
            LoadBalancerIngress loadBalancerIngress = extBootstrapService.getStatus().getLoadBalancer().getIngress().get(0);
            String result = loadBalancerIngress.getHostname();

            if (result == null) {
                result = loadBalancerIngress.getIp();
            }
            return result + ":9094";
        } else {
            throw new RuntimeException("Unexpected external bootstrap service for Kafka cluster " + clusterName);
        }
    }

    /**
     * Create keystore
     *
     * @param ca       certificate authority
     * @param cert     certificate
     * @param key      key
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

    /**
     * Use Keycloak Admin API to update Authorization Services 'decisionStrategy' on 'kafka' client to AFFIRMATIVE
     * link to bug -> https://issues.redhat.com/browse/KEYCLOAK-12640
     *
     * @throws IOException
     */
    static void fixBadlyImportedAuthzSettings() throws IOException {
        URI masterTokenEndpoint = URI.create("http://" + kubeClient().getNodeAddress() + ":" + Constants.HTTP_KEYCLOAK_DEFAULT_NODE_PORT + "/auth/realms/master/protocol/openid-connect/token");

        String token = loginWithUsernamePassword(masterTokenEndpoint,
                "admin", "admin", "admin-cli");

        String authorization = "Bearer " + token;

        // This is quite a round-about way but here it goes

        // We first need to identify the 'id' of the 'kafka' client by fetching the clients
        JsonNode clients = HttpUtil.get(URI.create("http://" + kubeClient().getNodeAddress() + ":" + Constants.HTTP_KEYCLOAK_DEFAULT_NODE_PORT + "/auth/admin/realms/kafka-authz/clients"),
                authorization, JsonNode.class);

        String id = null;

        // iterate over clients
        Iterator<JsonNode> it = clients.iterator();
        while (it.hasNext()) {
            JsonNode client = it.next();
            String clientId = client.get("clientId").asText();
            if ("kafka".equals(clientId)) {
                id = client.get("id").asText();
                break;
            }
        }

        if (id == null) {
            throw new IllegalStateException("It seems that 'kafka' client isn't configured");
        }

        URI authzUri = URI.create("http://" + kubeClient().getNodeAddress() + ":" + Constants.HTTP_KEYCLOAK_DEFAULT_NODE_PORT + "/auth/admin/realms/kafka-authz/clients/" + id + "/authz/resource-server");

        // Now we fetch from this client's resource-server the current configuration
        ObjectNode authzConf = (ObjectNode) HttpUtil.get(authzUri, authorization, JsonNode.class);

        // And we update the configuration and send it back
        authzConf.put("decisionStrategy", "AFFIRMATIVE");
        HttpUtil.put(authzUri, authorization, "application/json", authzConf.toString());
    }

    static String loginWithUsernamePassword(URI tokenEndpointUri, String username, String password, String clientId) throws IOException {
        StringBuilder body = new StringBuilder("grant_type=password&username=" + urlencode(username) +
                "&password=" + urlencode(password) + "&client_id=" + urlencode(clientId));

        JsonNode result = HttpUtil.post(tokenEndpointUri,
                null,
                null,
                null,
                "application/x-www-form-urlencoded",
                body.toString(),
                JsonNode.class);

        JsonNode token = result.get("access_token");
        if (token == null) {
            throw new IllegalStateException("Invalid response from authorization server: no access_token");
        }
        return token.asText();
    }
}
