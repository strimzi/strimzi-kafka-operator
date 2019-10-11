/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.oauth;

import io.fabric8.kubernetes.api.model.Service;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.utils.BridgeUtils;
import io.strimzi.systemtest.utils.HttpUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TimeoutException;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static io.strimzi.systemtest.Constants.HTTP_BRIDGE_DEFAULT_PORT;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.OAUTH;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@Tag(OAUTH)
@Tag(REGRESSION)
@Tag(NODEPORT_SUPPORTED)
public class OauthTlsST extends OauthBaseST {

    @Test
    void testProducerConsumer() {
        deployProducerWithOauthTls();
        deployConsumerWithOauthTls(TOPIC_NAME);

        String podName = kubeClient().listPodsByPrefixInName("hello-world-producer-").get(0).getMetadata().getName();
        String producerMessage = "Sending messages \"Hello world - " + END_MESSAGE_OFFSET + "\"";

        LOGGER.info("Waiting for:" + producerMessage);

        StUtils.waitUntilMessageIsInPodLogs(podName, producerMessage);

        String producerLogs = kubeClient().logs(podName);

        for (int i = START_MESSAGE_OFFSET; i < END_MESSAGE_OFFSET; i++) {
            assertThat("Producer doesn't send message" + i, producerLogs, containsString("Sending messages \"Hello world - " + i + "\""));
        }

        String consumerPodName = kubeClient().listPodsByPrefixInName("hello-world-consumer-").get(0).getMetadata().getName();
        String consumerMessage = "value: \"Hello world - " + END_MESSAGE_OFFSET + "\"";

        StUtils.waitUntilMessageIsInPodLogs(consumerPodName, consumerMessage);

        String consumerLogs = kubeClient().logs(consumerPodName);

        for (int i = START_MESSAGE_OFFSET; i < END_MESSAGE_OFFSET; i++) {
            assertThat("Producer doesn't send message" + i, consumerLogs, containsString("value: \"Hello world - " + i + "\""));
        }
    }

    @Test
    void testProducerConsumerStreams() {
        deployProducerWithOauthTls();
        deployConsumerWithOauthTls(REVERSE_TOPIC_NAME);
        deployKafkaStreamsOauthTls();

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
        deployProducerWithOauthTls();
        deployConsumerWithOauthTls(TOPIC_NAME);

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1)
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
                .withSecretName("my-connect-oauth")
                .withKey(OAUTH_KEY)
                .endClientSecret()
                .withTlsTrustedCertificates(
                        new CertSecretSourceBuilder()
                                .withSecretName(SECRET_OF_KEYCLOAK)
                                .withCertificate(CERTIFICATE_OF_KEYCLOAK)
                                .build())
                .withDisableTlsHostnameVerification(true)
                .endKafkaClientAuthenticationOAuth()
                .withNewTls()
                .addNewTrustedCertificate()
                .withSecretName(CLUSTER_NAME + "-cluster-ca-cert")
                .withCertificate("ca.crt")
                .endTrustedCertificate()
                .endTls()
                .withBootstrapServers(CLUSTER_NAME + "-kafka-bootstrap:9093")
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
    void testProducerConsumerBridge(Vertx vertx) throws InterruptedException, TimeoutException, ExecutionException, java.util.concurrent.TimeoutException {
        deployProducerWithOauthTls();
        deployConsumerWithOauthTls(TOPIC_NAME);

        testMethodResources().kafkaBridge(CLUSTER_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME), 1, HTTP_BRIDGE_DEFAULT_PORT)
                .editSpec()
                .withNewTls()
                .withTrustedCertificates(
                        new CertSecretSourceBuilder()
                                .withCertificate("ca.crt")
                                .withSecretName(clusterCaCertSecretName(CLUSTER_NAME)).build())
                .endTls()
                .withNewKafkaClientAuthenticationOAuth()
                .withTokenEndpointUri(oauthTokenEndpointUri)
                .withClientId("kafka-bridge")
                .withNewClientSecret()
                .withSecretName("my-bridge-oauth")
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

        Service bridgeService = testMethodResources().deployBridgeNodePortService(BRIDGE_EXTERNAL_SERVICE, NAMESPACE);
        testMethodResources().createServiceResource(bridgeService, NAMESPACE);

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

        JsonObject response = HttpUtils.sendHttpRequests(root, clusterHost, BridgeUtils.getBridgeNodePort(NAMESPACE, BRIDGE_EXTERNAL_SERVICE), TOPIC_NAME, client);

        response.getJsonArray("offsets").forEach(object -> {
            if (object instanceof JsonObject) {
                JsonObject item = (JsonObject) object;
                LOGGER.info("Offset number is {}", item.getInteger("offset"));
                int exceptedValue = 0;
                assertThat("Offset is not zero", item.getInteger("offset"), greaterThan(exceptedValue));
            }
        });
    }

    private void deployProducerWithOauthTls() {
        testMethodResources().producerWithOauth(oauthTokenEndpointUri, TOPIC_NAME, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
                .editSpec()
                .editTemplate()
                .editSpec()
                .editFirstContainer()
                .addNewEnv()
                .withName("CA_CRT")
                .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(CLUSTER_NAME + "-cluster-ca-cert")
                .withKey("ca.crt")
                .endSecretKeyRef()
                .endValueFrom()
                .endEnv()
                .addNewEnv()
                .withName("USER_CRT")
                .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(PRODUCER_USER_NAME)
                .withKey("user.crt")
                .endSecretKeyRef()
                .endValueFrom()
                .endEnv()
                .addNewEnv()
                .withName("USER_KEY")
                .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(PRODUCER_USER_NAME)
                .withKey("user.key")
                .endSecretKeyRef()
                .endValueFrom()
                .endEnv()
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .done();
    }

    private void deployConsumerWithOauthTls(String topicName) {
        testMethodResources().consumerWithOauth(oauthTokenEndpointUri, topicName, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
                .editSpec()
                .editTemplate()
                .editSpec()
                .editFirstContainer()
                .addNewEnv()
                .withName("CA_CRT")
                .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(CLUSTER_NAME + "-cluster-ca-cert")
                .withKey("ca.crt")
                .endSecretKeyRef()
                .endValueFrom()
                .endEnv()
                .addNewEnv()
                .withName("USER_CRT")
                .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(CONSUMER_USER_NAME)
                .withKey("user.crt")
                .endSecretKeyRef()
                .endValueFrom()
                .endEnv()
                .addNewEnv()
                .withName("USER_KEY")
                .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(CONSUMER_USER_NAME)
                .withKey("user.key")
                .endSecretKeyRef()
                .endValueFrom()
                .endEnv()
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .done();
    }

    private void deployKafkaStreamsOauthTls() {
        testMethodResources().kafkaStreamsWithOauth(oauthTokenEndpointUri, KafkaResources.tlsBootstrapAddress(CLUSTER_NAME))
                .editSpec()
                .editTemplate()
                .editSpec()
                .editFirstContainer()
                .addNewEnv()
                .withName("CA_CRT")
                .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(clusterCaCertSecretName(CLUSTER_NAME))
                .withKey("ca.crt")
                .endSecretKeyRef()
                .endValueFrom()
                .endEnv()
                .addNewEnv()
                .withName("USER_CRT")
                .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(STREAMS_USER_NAME)
                .withKey("user.crt")
                .endSecretKeyRef()
                .endValueFrom()
                .endEnv()
                .addNewEnv()
                .withName("USER_KEY")
                .withNewValueFrom()
                .withNewSecretKeyRef()
                .withName(STREAMS_USER_NAME)
                .withKey("user.key")
                .endSecretKeyRef()
                .endValueFrom()
                .endEnv()
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .done();
    }

}



