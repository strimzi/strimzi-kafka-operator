/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.openshift.client.OpenShiftClient;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.HttpUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.ServiceUtils;
import io.strimzi.systemtest.utils.specific.TracingUtils;
import io.strimzi.test.TestUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaBridgeResource;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectS2IResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.resources.crd.KafkaTopicResource;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;
import static io.strimzi.systemtest.Constants.NODEPORT_SUPPORTED;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.systemtest.Constants.TRACING;
import static io.strimzi.test.TestUtils.getFileAsString;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@Tag(NODEPORT_SUPPORTED)
@Tag(REGRESSION)
@Tag(TRACING)
@ExtendWith(VertxExtension.class)
public class TracingST extends BaseST {

    private static final String NAMESPACE = "tracing-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(TracingST.class);

    private static final String JI_INSTALL_DIR = "../systemtest/src/test/resources/tracing/jaeger-instance/";
    private static final String JO_INSTALL_DIR = "../systemtest/src/test/resources/tracing/jaeger-operator/";

    private static final String JAEGER_PRODUCER_SERVICE = "hello-world-producer";
    private static final String JAEGER_CONSUMER_SERVICE = "hello-world-consumer";
    private static final String JAEGER_KAFKA_STREAMS_SERVICE = "hello-world-streams";
    private static final String JAEGER_MIRROR_MAKER_SERVICE = "my-mirror-maker";
    private static final String JAEGER_KAFKA_CONNECT_SERVICE = "my-connect";
    private static final String JAEGER_KAFKA_CONNECT_S2I_SERVICE = "my-connect-s2i";
    private static final String JAEGER_KAFKA_BRIDGE_SERVICE = "my-kafka-bridge";
    private static final String BRIDGE_EXTERNAL_SERVICE = CLUSTER_NAME + "-bridge-external-service";

    private static int jaegerHostNodePort;

    private static final String JAEGER_AGENT_NAME = "my-jaeger-agent";
    private static final String JAEGER_SAMPLER_TYPE = "const";
    private static final String JAEGER_SAMPLER_PARAM = "1";

    private static final String TOPIC_NAME = "my-topic";
    private static final String TOPIC_TARGET_NAME = "cipot-ym";

    private Stack<String> jaegerConfigs = new Stack<>();

    @Test
    void testProducerService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(1)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaClientsResource.producerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_PRODUCER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }

    @Test
    void testConnectService() throws Exception {
        Map<String, Object> configOfKafka = new HashMap<>();
        configOfKafka.put("offsets.topic.replication.factor", "1");
        configOfKafka.put("transaction.state.log.replication.factor", "1");
        configOfKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                        .withConfig(configOfKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .withNewSpec()
                    .withConfig(configOfKafkaConnect)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                    .withReplicas(1)
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_CONNECT_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .done();

        String kafkaConnectPodName = kubeClient().listPods().stream().filter(pod -> pod.getMetadata().getName().startsWith(CLUSTER_NAME + "-connect")).findFirst().get().getMetadata().getName();
        String pathToConnectorSinkConfig = "../systemtest/src/test/resources/file/sink/connector.json";
        String connectorConfig = getFileAsString(pathToConnectorSinkConfig);

        LOGGER.info("Creating file sink in {}", pathToConnectorSinkConfig);
        cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "curl -X POST -H \"Content-Type: application/json\" --data "
                + "'" + connectorConfig + "'" + " http://localhost:8083/connectors");

        Future<Integer> producer = externalBasicKafkaClient.sendMessages(TEST_TOPIC_NAME, NAMESPACE, CLUSTER_NAME, MESSAGE_COUNT);
        Future<Integer> consumer = externalBasicKafkaClient.receiveMessages(TEST_TOPIC_NAME, NAMESPACE, CLUSTER_NAME, MESSAGE_COUNT);

        assertThat(producer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));
        assertThat(consumer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_KAFKA_CONNECT_SERVICE);

        verifyServiceIsPresent(JAEGER_KAFKA_CONNECT_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_KAFKA_CONNECT_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_KAFKA_CONNECT_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TEST_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TEST_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TEST_TOPIC_NAME);
    }

    @Test
    void testProducerWithStreamsService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaClientsResource.producerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_PRODUCER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE);

        KafkaClientsResource.kafkaStreamsWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_KAFKA_STREAMS_SERVICE);

        given()
                .when()
                    .relaxedHTTPSValidation()
                    .contentType("application/json")
                    .get("/jaeger/api/services")
                .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("data", hasItem(JAEGER_PRODUCER_SERVICE))
                    .body("data", hasItem(JAEGER_KAFKA_STREAMS_SERVICE))
                .log().all();

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_PRODUCER_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_KAFKA_STREAMS_SERVICE);

        LOGGER.info("Verifying that {} create some trace with spans", JAEGER_KAFKA_STREAMS_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_KAFKA_STREAMS_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    void testProducerConsumerService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaClientsResource.producerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_PRODUCER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE);

        KafkaClientsResource.consumerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_CONSUMER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_CONSUMER_SERVICE);

        verifyServiceIsPresent(JAEGER_CONSUMER_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE);

        verifyServicesHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }

    @Test
    void testProducerConsumerStreamsService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfSourceKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();


        KafkaTopicResource.topic(CLUSTER_NAME, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaClientsResource.producerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_PRODUCER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE);

        KafkaClientsResource.consumerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_CONSUMER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_CONSUMER_SERVICE);

        verifyServiceIsPresent(JAEGER_CONSUMER_SERVICE);

        KafkaClientsResource.kafkaStreamsWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_KAFKA_STREAMS_SERVICE);

        verifyServiceIsPresent(JAEGER_KAFKA_STREAMS_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_STREAMS_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_STREAMS_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    void testProducerConsumerMirrorMakerService() {
        Map<String, Object> configOfKafka = new HashMap<>();
        configOfKafka.put("offsets.topic.replication.factor", "1");
        configOfKafka.put("transaction.state.log.replication.factor", "1");
        configOfKafka.put("transaction.state.log.min.isr", "1");

        final String kafkaClusterSourceName = CLUSTER_NAME + "-source";
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 3, 1)
                .editSpec()
                    .editKafka()
                        .withConfig(configOfKafka)
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endKafka()
                    .editZookeeper()
                        .withNewPersistentClaimStorage()
                            .withNewSize("10")
                            .withDeleteClaim(true)
                        .endPersistentClaimStorage()
                    .endZookeeper()
                .endSpec()
                .done();

        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName,
                "my-group" + new Random().nextInt(Integer.MAX_VALUE), 1, false)
                .editMetadata()
                    .withName("my-mirror-maker")
                .endMetadata()
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewMirrorMakerContainer()
                            .addNewEnv()
                                .withNewName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_MIRROR_MAKER_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("AEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .done();

        KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaTopicResource.topic(kafkaClusterTargetName, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        KafkaClientsResource.producerWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName)).done();

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        KafkaClientsResource.consumerWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE,
                JAEGER_MIRROR_MAKER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_MIRROR_MAKER_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE,
                JAEGER_MIRROR_MAKER_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_MIRROR_MAKER_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    @SuppressWarnings({"checkstyle:MethodLength"})
    void testProducerConsumerMirrorMakerConnectStreamsService() throws Exception {
        Map<String, Object> configOfKafka = new HashMap<>();
        configOfKafka.put("offsets.topic.replication.factor", "1");
        configOfKafka.put("transaction.state.log.replication.factor", "1");
        configOfKafka.put("transaction.state.log.min.isr", "1");

        final String kafkaClusterSourceName = CLUSTER_NAME + "-source";
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        KafkaResource.kafkaEphemeral(kafkaClusterSourceName, 3, 1).editSpec().editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        KafkaResource.kafkaEphemeral(kafkaClusterTargetName, 3, 1).editSpec().editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName,
                "my-group" + new Random().nextInt(Integer.MAX_VALUE), 1, false)
                .editMetadata()
                    .withName("my-mirror-maker")
                .endMetadata()
                .editSpec()
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewMirrorMakerContainer()
                            .addNewEnv()
                                .withNewName("JAEGER_SERVICE_NAME")
                                .withValue("my-mirror-maker")
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .done();

        KafkaTopicResource.topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaTopicResource.topic(kafkaClusterTargetName, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        KafkaClientsResource.producerWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName)).done();
        KafkaClientsResource.consumerWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName)).done();
        KafkaClientsResource.kafkaStreamsWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName)).done();

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        KafkaConnectResource.kafkaConnect(CLUSTER_NAME, 1)
                .withNewSpec()
                    .withConfig(configOfKafkaConnect)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName))
                    .withReplicas(1)
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_CONNECT_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .done();

        String kafkaConnectPodName = kubeClient().listPods().stream().filter(pod -> pod.getMetadata().getName().startsWith(CLUSTER_NAME + "-connect")).findFirst().get().getMetadata().getName();
        String pathToConnectorSinkConfig = "../systemtest/src/test/resources/file/sink/connector.json";
        String connectorConfig = getFileAsString(pathToConnectorSinkConfig);

        LOGGER.info("Creating file sink in {}", pathToConnectorSinkConfig);
        cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "curl -X POST -H \"Content-Type: application/json\" --data "
                + "'" + connectorConfig + "'" + " http://localhost:8083/connectors");

        Future<Integer> producer = externalBasicKafkaClient.sendMessages(TEST_TOPIC_NAME, NAMESPACE, kafkaClusterTargetName, MESSAGE_COUNT);
        Future<Integer> consumer = externalBasicKafkaClient.receiveMessages(TEST_TOPIC_NAME, NAMESPACE, kafkaClusterTargetName, MESSAGE_COUNT);

        assertThat(producer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));
        assertThat(consumer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE,
                JAEGER_KAFKA_CONNECT_SERVICE, JAEGER_KAFKA_STREAMS_SERVICE, JAEGER_MIRROR_MAKER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_CONNECT_SERVICE,
                JAEGER_KAFKA_STREAMS_SERVICE, JAEGER_MIRROR_MAKER_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE,
                JAEGER_KAFKA_CONNECT_SERVICE, JAEGER_KAFKA_STREAMS_SERVICE, JAEGER_MIRROR_MAKER_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_CONNECT_SERVICE,
                JAEGER_KAFKA_STREAMS_SERVICE, JAEGER_MIRROR_MAKER_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TEST_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TEST_TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TEST_TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    void testConnectS2IService() throws Exception {
        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
                .editSpec()
                    .editKafka()
                        .editListeners()
                            .withNewKafkaListenerExternalNodePort()
                                .withTls(false)
                            .endKafkaListenerExternalNodePort()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .done();

        KafkaClientsResource.producerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();
        KafkaClientsResource.consumerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        final String kafkaConnectS2IName = "kafka-connect-s2i-name-1";

        Map<String, Object> configOfKafkaConnectS2I = new HashMap<>();
        configOfKafkaConnectS2I.put("key.converter.schemas.enable", "false");
        configOfKafkaConnectS2I.put("value.converter.schemas.enable", "false");
        configOfKafkaConnectS2I.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnectS2I.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        KafkaConnectS2IResource.kafkaConnectS2I(kafkaConnectS2IName, CLUSTER_NAME, 1)
                .editMetadata()
                    .addToLabels("type", "kafka-connect-s2i")
                .endMetadata()
                .editSpec()
                    .withConfig(configOfKafkaConnectS2I)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_CONNECT_S2I_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endConnectContainer()
                    .endTemplate()
                .endSpec()
                .done();

        String kafkaConnectS2IPodName = kubeClient().listPods("type", "kafka-connect-s2i").get(0).getMetadata().getName();
        String execPodName = KafkaResources.kafkaPodName(CLUSTER_NAME, 0);

        LOGGER.info("Creating FileSink connect via Pod:{}", execPodName);
        KafkaConnectUtils.createFileSinkConnector(execPodName, TEST_TOPIC_NAME, Constants.DEFAULT_SINK_FILE_NAME,
                KafkaConnectResources.url(kafkaConnectS2IName, NAMESPACE, 8083));

        Future<Integer> producer = externalBasicKafkaClient.sendMessages(TEST_TOPIC_NAME, NAMESPACE, CLUSTER_NAME, MESSAGE_COUNT);
        Future<Integer> consumer = externalBasicKafkaClient.receiveMessages(TEST_TOPIC_NAME, NAMESPACE, CLUSTER_NAME, MESSAGE_COUNT);

        assertThat(producer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));
        assertThat(consumer.get(2, TimeUnit.MINUTES), is(MESSAGE_COUNT));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectS2IPodName, Constants.DEFAULT_SINK_FILE_NAME);

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE,
                JAEGER_KAFKA_CONNECT_S2I_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_CONNECT_S2I_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_CONNECT_S2I_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_PRODUCER_SERVICE, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_CONNECT_S2I_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        KafkaTopicUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }

    @Test
    void testKafkaBridgeService(Vertx vertx) throws Exception {
        WebClient client = WebClient.create(vertx, new WebClientOptions().setSsl(false));

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3, 1)
            .editSpec()
                .editKafka()
                    .editListeners()
                        .withNewKafkaListenerExternalNodePort()
                            .withTls(false)
                        .endKafkaListenerExternalNodePort()
                    .endListeners()
                .endKafka()
            .endSpec()
            .done();

        // Deploy http bridge
        KafkaBridgeResource.kafkaBridge(CLUSTER_NAME, KafkaResources.plainBootstrapAddress(CLUSTER_NAME), 1)
            .editSpec()
                .withNewJaegerTracing()
                .endJaegerTracing()
                    .withNewTemplate()
                        .withNewBridgeContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue(JAEGER_KAFKA_BRIDGE_SERVICE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue(JAEGER_AGENT_NAME)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue(JAEGER_SAMPLER_TYPE)
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue(JAEGER_SAMPLER_PARAM)
                            .endEnv()
                        .endBridgeContainer()
                    .endTemplate()
            .endSpec()
            .done();

        Service service = KafkaBridgeUtils.createBridgeNodePortService(CLUSTER_NAME, NAMESPACE, BRIDGE_EXTERNAL_SERVICE);
        KubernetesResource.createServiceResource(service, NAMESPACE).done();
        ServiceUtils.waitForNodePortService(BRIDGE_EXTERNAL_SERVICE);

        int bridgePort = KafkaBridgeUtils.getBridgeNodePort(NAMESPACE, BRIDGE_EXTERNAL_SERVICE);
        String bridgeHost = kubeClient(NAMESPACE).getNodeAddress();

        int messageCount = 50;
        String topicName = "topic-simple-send";

        KafkaTopicResource.topic(CLUSTER_NAME, topicName).done();
        JsonObject records = HttpUtils.generateHttpMessages(messageCount);

        JsonObject response = HttpUtils.sendMessagesHttpRequest(records, bridgeHost, bridgePort, topicName, client);
        KafkaBridgeUtils.checkSendResponse(response, messageCount);

        Future<Integer> consumer = externalBasicKafkaClient.receiveMessages(topicName, NAMESPACE, CLUSTER_NAME, messageCount);
        assertThat(consumer.get(2, TimeUnit.MINUTES), is(messageCount));

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_KAFKA_BRIDGE_SERVICE);
        verifyServiceIsPresent(JAEGER_KAFKA_BRIDGE_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_KAFKA_BRIDGE_SERVICE);
        verifyServicesHasSomeTraces(JAEGER_KAFKA_BRIDGE_SERVICE);
    }

    private void verifyServiceIsPresent(String serviceName) {
        given()
                .when()
                    .relaxedHTTPSValidation()
                    .contentType("application/json")
                    .get("/jaeger/api/services")
                .then()
                    .statusCode(200)
                    .contentType(ContentType.JSON)
                    .body("data", hasItem(serviceName))
                .log().all();
    }

    private void verifyServiceIsPresent(String... serviceNames) {
        for (String serviceName : serviceNames) {
            verifyServiceIsPresent(serviceName);
        }
    }

    private void verifyServicesHasSomeTraces(String serviceName) {
        LOGGER.info("Verifying that {} create some trace with spans", serviceName);

        Response response = given()
                .when()
                    .relaxedHTTPSValidation()
                    .contentType("application/json")
                    .get("/jaeger/api/traces?service=" + serviceName);

        JsonPath jsonPathValidator = response.jsonPath();
        Map<Object, Object> data = jsonPathValidator.getMap("$");

        assertThat(serviceName + " service doesn't produce traces", data.size(), greaterThan(0));
    }

    private void verifyServicesHasSomeTraces(String... serviceNames) {
        for (String serviceName : serviceNames) {
            verifyServicesHasSomeTraces(serviceName);
        }
    }

    private void deployJaeger() {
        LOGGER.info("=== Applying jaeger operator install files ===");

        Map<File, String> operatorFiles = Arrays.stream(Objects.requireNonNull(new File(JO_INSTALL_DIR).listFiles())
        ).collect(Collectors.toMap(file -> file, f -> TestUtils.getContent(f, TestUtils::toYamlString), (x, y) -> x, LinkedHashMap::new));

        for (Map.Entry<File, String> entry : operatorFiles.entrySet()) {
            LOGGER.info("Applying configuration file: {}", entry.getKey());
            jaegerConfigs.push(entry.getValue());
            cmdKubeClient().clientWithAdmin().namespace(cluster.getNamespace()).applyContent(entry.getValue());
        }

        installJaegerInstance();
    }

    /**
     * Install of Jaeger instance
     */
    void installJaegerInstance() {
        LOGGER.info("=== Applying jaeger instance install files ===");

        Map<File, String> operatorFiles = Arrays.stream(Objects.requireNonNull(new File(JI_INSTALL_DIR).listFiles())
        ).collect(Collectors.toMap(file -> file, f -> TestUtils.getContent(f, TestUtils::toYamlString), (x, y) -> x, LinkedHashMap::new));

        for (Map.Entry<File, String> entry : operatorFiles.entrySet()) {
            LOGGER.info("Applying configuration file: {}", entry.getKey());
            jaegerConfigs.push(entry.getValue());
            cmdKubeClient().clientWithAdmin().namespace(cluster.getNamespace()).applyContent(entry.getValue());
        }
    }

    /**
     * Delete Jaeger instance
     */
    void deleteJaeger() {
        while (!jaegerConfigs.empty()) {
            cmdKubeClient().clientWithAdmin().namespace(cluster.getNamespace()).deleteContent(jaegerConfigs.pop());
        }
    }

    @AfterEach
    void tearDown() {
        deleteJaeger();
    }

    @BeforeEach
    void createTestResources() throws InterruptedException {
        // deployment of the jaeger
        deployJaeger();

        TestUtils.waitFor("Waiting till route {} is ready", Constants.GLOBAL_TRACING_POLL, Constants.GLOBAL_TRACING_TIMEOUT,
            () -> kubeClient(NAMESPACE).getClient().adapt(OpenShiftClient.class).routes().inNamespace(NAMESPACE).list().getItems().get(0).getSpec().getHost() != null);

        String jaegerRouteUrl = kubeClient(NAMESPACE).getClient().adapt(OpenShiftClient.class).routes().inNamespace(NAMESPACE)
                .list().getItems().get(0).getSpec().getHost();

        RestAssured.baseURI = "https://" + jaegerRouteUrl;

        LOGGER.info("Setting Jaeger URL to:" + RestAssured.baseURI);

        Service jaegerHostNodePortService = TracingUtils.createJaegerHostNodePortService(CLUSTER_NAME, NAMESPACE, JAEGER_AGENT_NAME + "-external");
        KubernetesResource.createServiceResource(jaegerHostNodePortService, NAMESPACE).done();
        ServiceUtils.waitForNodePortService(jaegerHostNodePortService.getMetadata().getName());

        jaegerHostNodePort = TracingUtils.getJaegerHostNodePort(NAMESPACE, JAEGER_AGENT_NAME + "-external");
    }

    @BeforeAll
    void setup() {
        ResourceManager.setClassResources();
        prepareEnvForOperator(NAMESPACE);

        applyRoleBindings(NAMESPACE);
        // 050-Deployment
        KubernetesResource.clusterOperator(NAMESPACE).done();
    }
}
