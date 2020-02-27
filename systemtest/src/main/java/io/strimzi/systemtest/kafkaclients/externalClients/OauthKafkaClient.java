/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.externalClients;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.IKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.test.executor.Exec;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.strimzi.systemtest.kafkaclients.externalClients.KafkaClientProperties.fixBadlyImportedAuthzSettings;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * The OauthKafkaClient for sending and receiving messages using access token provided by authorization server.
 * The client is using an external listeners.
 */
public class OauthKafkaClient implements IKafkaClient<Future<Integer>> {

    private Vertx vertx = Vertx.vertx();
    private static final Logger LOGGER = LogManager.getLogger(KafkaClient.class);

    private String caCertName;
    private String clientId;
    private String clientSecretName;
    private String oauthTokenEndpointUri;

    public Future<Integer> sendMessages(String topicName, String namespace, String clusterName, int messageCount) throws IOException {
        return sendMessages(topicName, namespace, clusterName, messageCount, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public Future<Integer> sendMessages(String topicName, String namespace, String clusterName, int messageCount,
                                        long timeoutMs) throws  IOException {
        String clientName = "sender-plain-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        vertx.deployVerticle(new Producer(KafkaClientProperties.createOauthProducerProperties(namespace, clusterName,
                this.clientId, this.clientSecretName, this.oauthTokenEndpointUri), resultPromise, msgCntPredicate,
                topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public Future<Integer> sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                           int messageCount, String securityProtocol) throws IOException {
        return sendMessagesTls(topicName, namespace, clusterName, kafkaUsername, messageCount, securityProtocol,
                Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public Future<Integer> sendMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                           int messageCount, String securityProtocol, long timeoutMs) throws IOException {
        String clientName = "sender-ssl" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        String caCertName = this.caCertName == null ?
                KafkaResource.getKafkaExternalListenerCaCertName(namespace, clusterName) : this.caCertName;
        LOGGER.info("Going to use the following CA certificate: {}", caCertName);

        vertx.deployVerticle(new Producer(KafkaClientProperties.createOauthProducerTlsProperties(namespace, clusterName,
                caCertName, kafkaUsername, securityProtocol, this.clientId, this.clientSecretName,
                this.oauthTokenEndpointUri), resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public Future<Integer> receiveMessages(String topicName, String namespace, String clusterName, int messageCount,
                                           String consumerGroup) throws IOException {
        return receiveMessages(topicName, namespace, clusterName, messageCount, consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public Future<Integer> receiveMessages(String topicName, String namespace, String clusterName, int messageCount,
                                           String consumerGroup, long timeoutMs) throws IOException {
        String clientName = "receiver-plain-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        vertx.deployVerticle(new Consumer(KafkaClientProperties.createOauthConsumerProperties(namespace, clusterName, consumerGroup,
                this.clientId, this.clientSecretName, this.oauthTokenEndpointUri), resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public Future<Integer> receiveMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                              int messageCount, String securityProtocol, String consumerGroup) throws IOException {
        return receiveMessagesTls(topicName, namespace, clusterName, kafkaUsername, messageCount, securityProtocol,
                consumerGroup, Constants.GLOBAL_CLIENTS_TIMEOUT);
    }

    @Override
    public Future<Integer> receiveMessagesTls(String topicName, String namespace, String clusterName, String kafkaUsername,
                                              int messageCount, String securityProtocol, String consumerGroup,
                                              long timeoutMs) throws IOException {
        String clientName = "receiver-ssl-" + clusterName;
        vertx = Vertx.vertx();
        CompletableFuture<Integer> resultPromise = new CompletableFuture<>();

        IntPredicate msgCntPredicate = x -> x == messageCount;

        String caCertName = this.caCertName == null ?
                KafkaResource.getKafkaExternalListenerCaCertName(namespace, clusterName) : this.caCertName;
        LOGGER.info("Going to use the following CA certificate: {}", caCertName);

        vertx.deployVerticle(new Consumer(KafkaClientProperties.createOauthTlsConsumerProperties(namespace, clusterName,
                caCertName, kafkaUsername, securityProtocol, consumerGroup, this.clientId, this.clientSecretName,
                this.oauthTokenEndpointUri),
                resultPromise, msgCntPredicate, topicName, clientName));

        try {
            resultPromise.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            resultPromise.completeExceptionally(e);
        }
        vertx.close();
        return resultPromise;
    }

    public String getCaCertName() {
        return caCertName;
    }

    public void setCaCertName(String caCertName) {
        this.caCertName = caCertName;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecretName() {
        return clientSecretName;
    }

    public void setClientSecretName(String clientSecretName) {
        this.clientSecretName = clientSecretName;
    }

    public String getOauthTokenEndpointUri() {
        return oauthTokenEndpointUri;
    }

    public void setOauthTokenEndpointUri(String oauthTokenEndpointUri) {
        this.oauthTokenEndpointUri = oauthTokenEndpointUri;
    }

    protected static Properties setOauthClientPlainProperties(Properties clientProperties, String clientId, String clientSecretName,
                                                              String oauthTokenEndpointUri) {
        LOGGER.info("Enabling Plaintext with setting {}={}", CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                "SASL_PLAINTEXT");

        clientProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        clientProperties.setProperty("sasl.mechanism", "OAUTHBEARER");
        clientProperties.setProperty("sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        clientProperties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule " +
                        "required " +
                        "oauth.client.id=\"" + clientId + "\" " +
                        "oauth.client.secret=\"" + clientSecretName + "\" " +
                        "oauth.token.endpoint.uri=\"" + oauthTokenEndpointUri + "\";");

        return clientProperties;
    }

    protected static Properties setOauthClientTlsProperties(Properties clientProperties, String clientId, String clientSecretName,
                                                            String oauthTokenEndpointUri) throws IOException {
        String responseKeycloak = Exec.exec("openssl", "s_client", "-showcerts", "-connect",
                kubeClient().getNodeAddress() + ":" + Constants.HTTPS_KEYCLOAK_DEFAULT_NODE_PORT).out();
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


        LOGGER.info("Enabling SSL with setting {}={}", CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        clientProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        clientProperties.setProperty("sasl.mechanism", "OAUTHBEARER");
        clientProperties.setProperty("sasl.login.callback.handler.class", "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        clientProperties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule " +
                        "required " +
                        "oauth.client.id=\"" + clientId + "\" " +
                        "oauth.client.secret=\"" + clientSecretName + "\" " +
                        "oauth.token.endpoint.uri=\"" + oauthTokenEndpointUri + "\" " +
                        "oauth.ssl.endpoint.identification.algorithm=\"\"" +
                        "oauth.ssl.truststore.location=\"" + clientProperties.get("ssl.truststore.location") + "\" " +
                        "oauth.ssl.truststore.password=\"" + clientProperties.get("ssl.truststore.password") + "\" " +
                        "oauth.ssl.truststore.type=\"" + clientProperties.get("ssl.truststore.type") + "\" ;");

        fixBadlyImportedAuthzSettings();

        return clientProperties;
    }

    @Override
    public String toString() {
        return "OauthKafkaClient{" +
                "vertx=" + vertx +
                ", caCertName='" + caCertName + '\'' +
                ", clientId='" + clientId + '\'' +
                ", clientSecretName='" + clientSecretName + '\'' +
                ", oauthTokenEndpointUri='" + oauthTokenEndpointUri + '\'' +
                '}';
    }
}
