/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.tracing;

import io.fabric8.openshift.client.OpenShiftClient;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.utils.HttpUtils;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Stack;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;

import static io.strimzi.systemtest.Constants.TRACING;
import static io.strimzi.test.TestUtils.getFileAsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.greaterThan;

@Tag(TRACING)
public class TracingST extends AbstractST {

    private static final String NAMESPACE = "tracing-cluster-test";
    private static final Logger LOGGER = LogManager.getLogger(TracingST.class);
    private static final String JI_INSTALL_DIR = "../systemtest/src/test/resources/tracing/jaeger-instance/";

    private static final String JAEGER_PRODUCER_SERVICE = "hello-world-producer";
    private static final String JAEGER_CONSUMER_SERVICE = "hello-world-consumer";
    private static final String JAEGER_KAFKA_STREAMS_SERVICE = "hello-world-streams";
    private static final String JAEGER_MIRROR_MAKER_SERVICE = "my-mirror-maker";
    private static final String JAEGER_KAFKA_CONNECT_SERVICE = "my-target-connect";

    private static final String TOPIC_NAME = "my-topic";
    private static final String TOPIC_TARGET_NAME = "cipot-ym";

    private Stack<String> jaegerConfigs = new Stack<>();

    @Test
    void testJaegerService() {

        given()
            .when()
                .relaxedHTTPSValidation()
                .contentType("application/json").get("/jaeger/api/services")
            .then()
                .statusCode(200)
                .body(anything())
            .log().all();
    }

    @Test
    void testProducerService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
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

        testMethodResources().topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        testMethodResources().producerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_PRODUCER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        StUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }

    @Test
    void testConnectService() {
        Map<String, Object> configOfKafka = new HashMap<>();
        configOfKafka.put("offsets.topic.replication.factor", "1");
        configOfKafka.put("transaction.state.log.replication.factor", "1");
        configOfKafka.put("transaction.state.log.min.isr", "1");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
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

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1)
                .editSpec()
                    .withConfig(configOfKafkaConnect)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue("my-target-connect")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue("my-jaeger-agent")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue("const")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue("1")
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

        sendMessages(kafkaConnectPodName, CLUSTER_NAME, CLUSTER_NAME + "-connect", TEST_TOPIC_NAME, 10);

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_KAFKA_CONNECT_SERVICE);

        verifyServiceIsPresent(JAEGER_KAFKA_CONNECT_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_KAFKA_CONNECT_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_KAFKA_CONNECT_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TEST_TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TEST_TOPIC_NAME);
        StUtils.waitForKafkaTopicDeletion(TEST_TOPIC_NAME);
    }

    @Test
    void testProducerWithStreamsService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
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

        testMethodResources().topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        testMethodResources().topic(CLUSTER_NAME, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        testMethodResources().producerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_PRODUCER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE);

        testMethodResources().kafkaStreamsWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

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
        StUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        StUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    void testProducerConsumerService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
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

        testMethodResources().topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        testMethodResources().producerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_PRODUCER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE);

        testMethodResources().consumerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_CONSUMER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_CONSUMER_SERVICE);

        verifyServiceIsPresent(JAEGER_CONSUMER_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE);

        verifyServicesHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        StUtils.waitForKafkaTopicDeletion(TOPIC_NAME);
    }

    @Test
    void testProducerConsumerStreamsService() {
        Map<String, Object> configOfSourceKafka = new HashMap<>();
        configOfSourceKafka.put("offsets.topic.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.replication.factor", "1");
        configOfSourceKafka.put("transaction.state.log.min.isr", "1");

        testMethodResources().kafkaEphemeral(CLUSTER_NAME, 1, 1)
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

        testMethodResources().topic(CLUSTER_NAME, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();


        testMethodResources().topic(CLUSTER_NAME, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        testMethodResources().producerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_PRODUCER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE);

        testMethodResources().consumerWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_CONSUMER_SERVICE);

        LOGGER.info("Verifying {} service", JAEGER_CONSUMER_SERVICE);

        verifyServiceIsPresent(JAEGER_CONSUMER_SERVICE);

        testMethodResources().kafkaStreamsWithTracing(KafkaResources.plainBootstrapAddress(CLUSTER_NAME)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_KAFKA_STREAMS_SERVICE);

        verifyServiceIsPresent(JAEGER_KAFKA_STREAMS_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_STREAMS_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_KAFKA_STREAMS_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        StUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        StUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    void testProducerConsumerMirrorMakerService() {
        Map<String, Object> configOfKafka = new HashMap<>();
        configOfKafka.put("offsets.topic.replication.factor", "1");
        configOfKafka.put("transaction.state.log.replication.factor", "1");
        configOfKafka.put("transaction.state.log.min.isr", "1");

        final String kafkaClusterSourceName = CLUSTER_NAME + "-source";
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        testMethodResources().kafkaEphemeral(kafkaClusterSourceName, 1, 1)
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

        testMethodResources().kafkaEphemeral(kafkaClusterTargetName, 1, 1)
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

        testMethodResources().kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName,
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
                                .withValue("my-jaeger-agent")
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue("const")
                            .endEnv()
                            .addNewEnv()
                                .withNewName("AEGER_SAMPLER_PARAM")
                                .withValue("1")
                            .endEnv()
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .done();

        testMethodResources().topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        testMethodResources().topic(kafkaClusterTargetName, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterSourceName));

        testMethodResources().producerWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName)).done();

        LOGGER.info("Setting for kafka source plain bootstrap:{}", KafkaResources.plainBootstrapAddress(kafkaClusterTargetName));

        testMethodResources().consumerWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName)).done();

        HttpUtils.waitUntilServiceWithNameIsReady(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE,
                JAEGER_MIRROR_MAKER_SERVICE);

        verifyServiceIsPresent(JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_MIRROR_MAKER_SERVICE);

        HttpUtils.waitUntilServiceHasSomeTraces(RestAssured.baseURI, JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE,
                JAEGER_MIRROR_MAKER_SERVICE);

        verifyServicesHasSomeTraces(JAEGER_PRODUCER_SERVICE, JAEGER_CONSUMER_SERVICE, JAEGER_MIRROR_MAKER_SERVICE);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        StUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        StUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
    }

    @Test
    void testProducerConsumerMirrorMakerConnectStreamsService() {
        Map<String, Object> configOfKafka = new HashMap<>();
        configOfKafka.put("offsets.topic.replication.factor", "1");
        configOfKafka.put("transaction.state.log.replication.factor", "1");
        configOfKafka.put("transaction.state.log.min.isr", "1");

        final String kafkaClusterSourceName = CLUSTER_NAME + "-source";
        final String kafkaClusterTargetName = CLUSTER_NAME + "-target";

        testMethodResources().kafkaEphemeral(kafkaClusterSourceName, 1, 1).done();
        testMethodResources().kafkaEphemeral(kafkaClusterTargetName, 1, 1).done();

        testMethodResources().kafkaMirrorMaker(CLUSTER_NAME, kafkaClusterSourceName, kafkaClusterTargetName,
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
                                .withValue("my-jaeger-agent")
                            .endEnv()
                            .addNewEnv()
                                .withNewName("JAEGER_SAMPLER_TYPE")
                                .withValue("const")
                            .endEnv()
                            .addNewEnv()
                                .withNewName("AEGER_SAMPLER_PARAM")
                                .withValue("1")
                            .endEnv()
                        .endMirrorMakerContainer()
                    .endTemplate()
                .endSpec()
                .done();

        testMethodResources().topic(kafkaClusterSourceName, TOPIC_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        testMethodResources().topic(kafkaClusterTargetName, TOPIC_TARGET_NAME)
                .editSpec()
                    .withReplicas(3)
                    .withPartitions(12)
                .endSpec()
                .done();

        testMethodResources().producerWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName)).done();
        testMethodResources().consumerWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterTargetName)).done();
        testMethodResources().kafkaStreamsWithTracing(KafkaResources.plainBootstrapAddress(kafkaClusterSourceName)).done();

        Map<String, Object> configOfKafkaConnect = new HashMap<>();
        configOfKafkaConnect.put("config.storage.replication.factor", "1");
        configOfKafkaConnect.put("offset.storage.replication.factor", "1");
        configOfKafkaConnect.put("status.storage.replication.factor", "1");
        configOfKafkaConnect.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");
        configOfKafkaConnect.put("key.converter.schemas.enable", "false");
        configOfKafkaConnect.put("value.converter.schemas.enable", "false");

        testMethodResources().kafkaConnect(CLUSTER_NAME, 1)
                .editSpec()
                    .withConfig(configOfKafkaConnect)
                    .withNewJaegerTracing()
                    .endJaegerTracing()
                    .withBootstrapServers(KafkaResources.plainBootstrapAddress(CLUSTER_NAME))
                    .withNewTemplate()
                        .withNewConnectContainer()
                            .addNewEnv()
                                .withName("JAEGER_SERVICE_NAME")
                                .withValue("my-target-connect")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_AGENT_HOST")
                                .withValue("my-jaeger-agent")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_TYPE")
                                .withValue("const")
                            .endEnv()
                            .addNewEnv()
                                .withName("JAEGER_SAMPLER_PARAM")
                                .withValue("1")
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

        sendMessages(kafkaConnectPodName, kafkaClusterTargetName, CLUSTER_NAME + "-connect", TEST_TOPIC_NAME, 10);

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
        StUtils.waitForKafkaTopicDeletion(TEST_TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_NAME);
        StUtils.waitForKafkaTopicDeletion(TOPIC_NAME);

        LOGGER.info("Deleting topic {} from CR", TOPIC_TARGET_NAME);
        cmdKubeClient().deleteByName("kafkatopic", TOPIC_TARGET_NAME);
        StUtils.waitForKafkaTopicDeletion(TOPIC_TARGET_NAME);
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
        cmdKubeClient().apply(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/crds/jaegertracing_v1_jaeger_crd.yaml", NAMESPACE));
        cmdKubeClient().apply(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/service_account.yaml", NAMESPACE));
        cmdKubeClient().apply(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/role.yaml", NAMESPACE));
        cmdKubeClient().apply(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/role_binding.yaml", NAMESPACE));
        cmdKubeClient().apply(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/operator.yaml", NAMESPACE));

        installJaegerInstance();
    }

    /**
     * Install of Jaeger instance
     */
    void installJaegerInstance() {
        LOGGER.info("=== Applying jaeger operator install files ===");

        Map<File, String> operatorFiles = Arrays.stream(Objects.requireNonNull(new File(JI_INSTALL_DIR).listFiles())
        ).collect(Collectors.toMap(file -> file, f -> TestUtils.getContent(f, TestUtils::toYamlString), (x, y) -> x, LinkedHashMap::new));

        for (Map.Entry<File, String> entry : operatorFiles.entrySet()) {
            LOGGER.info("Applying configuration file: {}", entry.getKey());
            jaegerConfigs.push(entry.getValue());
            cmdKubeClient().clientWithAdmin().namespace(getNamespace()).applyContent(entry.getValue());
        }
    }

    /**
     * Delete Jaeger instance
     */
    void deleteJaegerInstance() {
        while (!jaegerConfigs.empty()) {
            cmdKubeClient().clientWithAdmin().namespace(getNamespace()).deleteContent(jaegerConfigs.pop());
        }
        cmdKubeClient().delete(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/crds/jaegertracing_v1_jaeger_crd.yaml", NAMESPACE));
        cmdKubeClient().delete(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/service_account.yaml", NAMESPACE));
        cmdKubeClient().delete(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/role.yaml", NAMESPACE));
        cmdKubeClient().delete(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/role_binding.yaml", NAMESPACE));
        cmdKubeClient().delete(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/jaegertracing/jaeger-operator/master/deploy/operator.yaml", NAMESPACE));

    }

    @AfterEach
    void tearDown() {
        deleteJaegerInstance();
    }

    @BeforeEach
    void createTestResources()  {
        createTestMethodResources();

        // deployment of the jaeger
        deployJaeger();

        TestUtils.waitFor("Waiting till route {} is ready", Constants.GLOBAL_TRACING_POLL, Constants.GLOBAL_TRACING_TIMEOUT,
            () -> kubeClient(NAMESPACE).getClient().adapt(OpenShiftClient.class).routes().inNamespace(NAMESPACE).list().getItems().get(0).getSpec().getHost() != null);

        String jaegerRouteUrl = kubeClient(NAMESPACE).getClient().adapt(OpenShiftClient.class).routes().inNamespace(NAMESPACE)
                .list().getItems().get(0).getSpec().getHost();

        RestAssured.baseURI = "https://" + jaegerRouteUrl;

        LOGGER.info("Setting Jaeger URL to:" + RestAssured.baseURI);
    }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        applyRoleBindings(NAMESPACE);

        // 050-Deployment
        testClassResources().clusterOperator(NAMESPACE).done();

    }
}
