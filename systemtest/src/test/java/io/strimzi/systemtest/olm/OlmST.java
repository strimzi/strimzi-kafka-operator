/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.systemtest.BaseST;
import io.strimzi.systemtest.resources.OlmResource;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectS2IUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OlmST extends BaseST {

    public static final String NAMESPACE = "olm-namespace";

    private static final Logger LOGGER = LogManager.getLogger(OlmST.class);

    @Test
    @Order(1)
    void testDeployExampleKafka() {
        JsonObject kafkaResource = OlmResource.getExampleResources().get("Kafka");
        cmdKubeClient().applyContent(kafkaResource.toString());
        KafkaUtils.waitUntilKafkaCRIsReady(kafkaResource.getJsonObject("metadata").getString("name"));
    }

    @Test
    @Order(2)
    void testDeployExampleKafkaUser() {
        JsonObject kafkaUserResource = OlmResource.getExampleResources().get("KafkaUser");
        cmdKubeClient().applyContent(kafkaUserResource.toString());
        KafkaUserUtils.waitForKafkaUserCreation(kafkaUserResource.getJsonObject("metadata").getString("name"));
    }

    @Test
    @Order(3)
    void testDeployExampleKafkaTopic() {
        JsonObject kafkaTopicResource = OlmResource.getExampleResources().get("KafkaTopic");
        cmdKubeClient().applyContent(kafkaTopicResource.toString());
        KafkaTopicUtils.waitForKafkaTopicCreation(kafkaTopicResource.getJsonObject("metadata").getString("name"));
    }

    @Test
    @Order(4)
    void testDeployExampleKafkaConnect() {
        JsonObject kafkaConnectResource = OlmResource.getExampleResources().get("KafkaConnect");
        cmdKubeClient().applyContent(kafkaConnectResource.toString());
        KafkaConnectUtils.waitForConnectStatus(kafkaConnectResource.getJsonObject("metadata").getString("name"), "Ready");
    }

    @Test
    @Order(5)
    void testDeployExampleKafkaConnectS2I() {
        Map<String, JsonObject> examples = OlmResource.getExampleResources();
        JsonObject kafkaConnectS2IResource = examples.get("KafkaConnectS2I");
        kafkaConnectS2IResource.getJsonObject("metadata").put("name", "my-connect-s2i-cluster");
        kafkaConnectS2IResource.getJsonObject("spec").put("insecureSourceRepository", true);
        examples.put("KafkaConnectS2I", kafkaConnectS2IResource);
        OlmResource.setExampleResources(examples);
        cmdKubeClient().applyContent(kafkaConnectS2IResource.toString());
        KafkaConnectS2IUtils.waitForConnectS2IStatus(kafkaConnectS2IResource.getJsonObject("metadata").getString("name"), "Ready");
    }

    @Test
    @Order(6)
    void testDeployExampleKafkaBridge() {
        JsonObject kafkaBridgeResource = OlmResource.getExampleResources().get("KafkaBridge");
        cmdKubeClient().applyContent(kafkaBridgeResource.toString());
        KafkaBridgeUtils.waitUntilKafkaBridgeStatus(kafkaBridgeResource.getJsonObject("metadata").getString("name"), "Ready");
    }

    @Test
    @Order(7)
    void testDeployExampleKafkaMirrorMaker() {
        JsonObject kafkaMirroMakerResource = OlmResource.getExampleResources().get("KafkaMirrorMaker");
        cmdKubeClient().applyContent(kafkaMirroMakerResource.toString()
                .replace("my-source-cluster-kafka-bootstrap", "my-cluster-kafka-bootstrap")
                .replace("my-target-cluster-kafka-bootstrap", "my-cluster-kafka-bootstrap"));
        KafkaMirrorMakerUtils.waitUntilKafkaMirrorMakerStatus(kafkaMirroMakerResource.getJsonObject("metadata").getString("name"), "Ready");
    }

    @Test
    @Order(8)
    void testDeployExampleKafkaMirrorMaker2() {
        JsonObject kafkaMirrorMaker2Resource = OlmResource.getExampleResources().get("KafkaMirrorMaker2");
        cmdKubeClient().applyContent(kafkaMirrorMaker2Resource.toString()
                .replace("my-cluster-source-kafka-bootstrap", "my-cluster-kafka-bootstrap")
                .replace("my-cluster-target-kafka-bootstrap", "my-cluster-kafka-bootstrap"));
        KafkaMirrorMaker2Utils.waitUntilKafkaMirrorMaker2Status(kafkaMirrorMaker2Resource.getJsonObject("metadata").getString("name"), "Ready");

    }

    @BeforeAll
    void setup() throws Exception {
        ResourceManager.setClassResources();
        cluster.setNamespace(cluster.getDefaultOlmvNamespace());
        OlmResource.clusterOperator(cluster.getDefaultOlmvNamespace());
    }

    @AfterAll
    void teardown() {
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get("KafkaMirrorMaker2").toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get("KafkaMirrorMaker").toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get("KafkaBridge").toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get("KafkaConnectS2I").toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get("KafkaConnect").toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get("KafkaTopic").toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get("KafkaUser").toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get("Kafka").toString());
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
    }
}
