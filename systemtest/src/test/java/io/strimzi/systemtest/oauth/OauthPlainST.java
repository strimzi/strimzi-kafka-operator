/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.oauth;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.BridgeUtils;
import io.strimzi.systemtest.utils.HttpUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
public class OauthPlainST extends OauthBaseST {

    @Test
    void testProducerConsumer() {
        KafkaClientsResource.producerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();
        KafkaClientsResource.consumerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        String producerPodName = kubeClient().listPodsByPrefixInName("hello-world-producer-").get(0).getMetadata().getName();
        String producerMessage = "Sending messages \"Hello world - " + END_MESSAGE_OFFSET + "\"";

        StUtils.waitUntilMessageIsInPodLogs(producerPodName, producerMessage);

        String producerLogs = kubeClient().logs(producerPodName);

        for (int i = START_MESSAGE_OFFSET; i < END_MESSAGE_OFFSET; i++) {
            assertThat("Producer doesn't send message" + i, producerLogs, containsString("Sending messages \"Hello world - " + i + "\""));
        }

        String consumerPodName = kubeClient().listPodsByPrefixInName("hello-world-consumer-").get(0).getMetadata().getName();
        String consumerMessage = "value: \"Hello world - " + END_MESSAGE_OFFSET + "\"";

        StUtils.waitUntilMessageIsInPodLogs(consumerPodName, consumerMessage);

        String consumerLogs = kubeClient().logs(consumerPodName);

        for (int i = 0; i < END_MESSAGE_OFFSET; i++) {
            assertThat("Producer doesn't send message" + i, consumerLogs, containsString("value: \"Hello world - " + i + "\""));
        }
    }

    @Test
    void testProducerConsumerStreams() {
        KafkaClientsResource.producerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();
        KafkaClientsResource.consumerWithOauth(oauthTokenEndpointUri, "my-topic-reversed", KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();
        KafkaClientsResource.kafkaStreamsWithOauth(oauthTokenEndpointUri, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        String producerPodName = kubeClient().listPodsByPrefixInName("hello-world-producer-").get(0).getMetadata().getName();
        String producerMessage = "Sending messages \"Hello world - " + END_MESSAGE_OFFSET + "\"";

        StUtils.waitUntilMessageIsInPodLogs(producerPodName, producerMessage);

        String producerLogs = kubeClient().logs(producerPodName);

        for (int i = START_MESSAGE_OFFSET; i < END_MESSAGE_OFFSET; i++) {
            assertThat("Producer doesn't send message" + i, producerLogs, containsString("Sending messages \"Hello world - " + i + "\""));
        }

        String consumerPodName = kubeClient().listPodsByPrefixInName("hello-world-consumer-").get(0).getMetadata().getName();
        String consumerMessage = "value: \"" + reverseNumber(END_MESSAGE_OFFSET) + " - dlrow olleH\"";

        StUtils.waitUntilMessageIsInPodLogs(consumerPodName, consumerMessage);

        String consumerLogs = kubeClient().logs(consumerPodName);

        for (int i = START_MESSAGE_OFFSET; i < END_MESSAGE_OFFSET; i++) {
            assertThat("Producer doesn't send message" + i, consumerLogs, containsString("value: \"" + reverseNumber(i) + " - dlrow olleH\""));
        }
    }

    @Test
    void testProducerConsumerConnect() {
        KafkaClientsResource.producerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();
        KafkaClientsResource.consumerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .withNewKafkaClientAuthenticationOAuth()
                        .withTokenEndpointUri(oauthTokenEndpointUri)
                        .withClientId("kafka-connect")
                        .withNewClientSecret()
                            .withSecretName(CONNECT_OAUTH_SECRET)
                            .withKey(OAUTH_KEY)
                        .endClientSecret()
                        .withTlsTrustedCertificates(
                            new CertSecretSourceBuilder()
                                    .withSecretName(SECRET_OF_KEYCLOAK)
                                    .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                    .build())
                        .withDisableTlsHostnameVerification(true)
                    .endKafkaClientAuthenticationOAuth()
                .endSpec()
                .done();

        String kafkaConnectPodName = kubeClient().listPods("type", "kafka-connect").get(0).getMetadata().getName();

        StUtils.createFileSinkConnector(kafkaConnectPodName, TOPIC_NAME);

        String message = "Hello world - " + END_MESSAGE_OFFSET;

        StUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, message);

        assertThat(cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "cat /tmp/test-file-sink.txt").out(),
                containsString(message));
    }

    @Test
    void testProducerConsumerMirrorMaker() {
        KafkaClientsResource.producerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();
        KafkaClientsResource.consumerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        String targetKafkaCluster = CLUSTER_NAME + "-target";

        KafkaResource.kafkaEphemeral(targetKafkaCluster, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewPlain()
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(validIssuerUri)
                                    .withJwksEndpointUri(jwksEndpointUri)
                                    .withJwksExpirySeconds(500)
                                    .withJwksRefreshSeconds(400)
                                    .withUserNameClaim(userNameClaim)
                                    .withTlsTrustedCertificates(
                                        new CertSecretSourceBuilder()
                                            .withSecretName(SECRET_OF_KEYCLOAK)
                                            .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                            .build())
                                    .withDisableTlsHostnameVerification(true)
                                .endKafkaListenerAuthenticationOAuth()
                            .endPlain()
                            .withNewTls()
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(validIssuerUri)
                                    .withJwksEndpointUri(jwksEndpointUri)
                                    .withJwksExpirySeconds(500)
                                    .withJwksRefreshSeconds(400)
                                    .withUserNameClaim(userNameClaim)
                                    .withTlsTrustedCertificates(
                                        new CertSecretSourceBuilder()
                                            .withSecretName(SECRET_OF_KEYCLOAK)
                                            .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                            .build())
                                    .withDisableTlsHostnameVerification(true)
                                .endKafkaListenerAuthenticationOAuth()
                            .endTls()
                            .withNewKafkaListenerExternalNodePort()
                                .withNewKafkaListenerAuthenticationOAuth()
                                    .withValidIssuerUri(validIssuerUri)
                                    .withJwksExpirySeconds(500)
                                    .withJwksRefreshSeconds(400)
                                    .withJwksEndpointUri(jwksEndpointUri)
                                    .withUserNameClaim(userNameClaim)
                                    .withTlsTrustedCertificates(
                                        new CertSecretSourceBuilder()
                                            .withSecretName(SECRET_OF_KEYCLOAK)
                                            .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                            .build())
                                    .withDisableTlsHostnameVerification(true)
                                .endKafkaListenerAuthenticationOAuth()
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, CLUSTER_NAME, targetKafkaCluster,
                "my-group" +  new Random().nextInt(Integer.MAX_VALUE), 1, false)
                .editSpec()
                    .editConsumer()
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(oauthTokenEndpointUri)
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            .addNewTlsTrustedCertificate()
                                .withSecretName(SECRET_OF_KEYCLOAK)
                                .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                            .endTlsTrustedCertificate()
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaClientAuthenticationOAuth()
                    .endConsumer()
                    .editProducer()
                        .withNewKafkaClientAuthenticationOAuth()
                            .withNewTokenEndpointUri(oauthTokenEndpointUri)
                            .withClientId("kafka-mirror-maker")
                            .withNewClientSecret()
                                .withSecretName(MIRROR_MAKER_OAUTH_SECRET)
                                .withKey(OAUTH_KEY)
                            .endClientSecret()
                            .addNewTlsTrustedCertificate()
                                .withSecretName(SECRET_OF_KEYCLOAK)
                                .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                            .endTlsTrustedCertificate()
                            .withDisableTlsHostnameVerification(true)
                        .endKafkaClientAuthenticationOAuth()
                    .endProducer()
                .endSpec()
                .done();

        KafkaClientsResource.consumerWithOauth("hello-world-consumer-target", oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(targetKafkaCluster)).done();

        String consumerPodName = kubeClient().listPodsByPrefixInName("hello-world-consumer-target-").get(0).getMetadata().getName();
        String consumerMessage = "value: \"Hello world - " + END_MESSAGE_OFFSET + "\"";

        StUtils.waitUntilMessageIsInPodLogs(consumerPodName, consumerMessage);

        String consumerLogs = kubeClient().logs(consumerPodName);

        for (int i = START_MESSAGE_OFFSET; i < END_MESSAGE_OFFSET; i++) {
            assertThat("MirrorMaker doesn't replicated data to target kafka cluster", consumerLogs, containsString("value: \"Hello world - " + i + "\""));
        }
    }

    @Test
    void testProducerConsumerBridge(Vertx vertx) throws InterruptedException, ExecutionException, TimeoutException {
        KafkaClientsResource.producerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();
        KafkaClientsResource.consumerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1)
                .editSpec()
                    .withNewKafkaClientAuthenticationOAuth()
                        .withTokenEndpointUri(oauthTokenEndpointUri)
                        .withClientId("kafka-bridge")
                        .withNewClientSecret()
                            .withSecretName(BRIDGE_OAUTH_SECRET)
                            .withKey(OAUTH_KEY)
                        .endClientSecret()
                        .addNewTlsTrustedCertificate()
                            .withSecretName(SECRET_OF_KEYCLOAK)
                            .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                        .endTlsTrustedCertificate()
                        .withDisableTlsHostnameVerification(true)
                    .endKafkaClientAuthenticationOAuth()
                .endSpec()
                .done();

        Service bridgeService = KubernetesResource.deployBridgeNodePortService(BRIDGE_EXTERNAL_SERVICE, NAMESPACE, CLUSTER_NAME);
        KubernetesResource.createServiceResource(bridgeService, NAMESPACE);

        StUtils.waitForNodePortService(bridgeService.getMetadata().getName());

        client = WebClient.create(vertx, new WebClientOptions().setSsl(false));

        JsonObject obj = new JsonObject();
        obj.put("key", "my-key");

        JsonArray records = new JsonArray();

        JsonObject firstLead = new JsonObject();
        firstLead.put("key", "my-key");
        firstLead.put("value", "sales-lead-0001");

        JsonObject secondLead = new JsonObject();
        secondLead.put("value", "sales-lead-0002");

        JsonObject thirdLead = new JsonObject();
        thirdLead.put("value", "sales-lead-0003");

        records.add(firstLead);
        records.add(secondLead);
        records.add(thirdLead);

        JsonObject root = new JsonObject();
        root.put("records", records);

        JsonObject response = HttpUtils.sendMessagesHttpRequest(root, clusterHost, BridgeUtils.getBridgeNodePort(NAMESPACE, BRIDGE_EXTERNAL_SERVICE), TOPIC_NAME, client);

        response.getJsonArray("offsets").forEach(object -> {
            if (object instanceof JsonObject) {
                JsonObject item = (JsonObject) object;
                LOGGER.info("Offset number is {}", item.getInteger("offset"));
                int exceptedValue = 0;
                assertThat("Offset is not zero", item.getInteger("offset"), greaterThan(exceptedValue));
            }
        });
    }
}
