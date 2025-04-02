/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;

import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class OlmAbstractST extends AbstractST {
    // Examples are assigned in respective test classes -> OlmAllNamespaceST and OlmSingleNamespaceST
    Map<String, JsonObject> exampleResources;

    void doTestDeployExampleKafka() {
        JsonObject kafkaResource = exampleResources.get(Kafka.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaResource.toString());
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, kafkaResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaUser() {
        String userKafkaName = "user-kafka";
        // KafkaUser example needs Kafka with authorization
        resourceManager.createResourceWithWait(KafkaTemplates.kafka(Environment.TEST_SUITE_NAMESPACE, userKafkaName, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                .endKafka()
            .endSpec()
            .build());
        JsonObject kafkaUserResource = exampleResources.get(KafkaUser.RESOURCE_KIND);
        kafkaUserResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, userKafkaName);
        cmdKubeClient().applyContent(kafkaUserResource.toString());
        KafkaUserUtils.waitForKafkaUserCreation(Environment.TEST_SUITE_NAMESPACE, kafkaUserResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaTopic() {
        JsonObject kafkaTopicResource = exampleResources.get(KafkaTopic.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaTopicResource.toString());
        KafkaTopicUtils.waitForKafkaTopicCreation(Environment.TEST_SUITE_NAMESPACE, kafkaTopicResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaConnect() {
        JsonObject kafkaConnectResource = exampleResources.get(KafkaConnect.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaConnectResource.toString());
        KafkaConnectUtils.waitForConnectReady(Environment.TEST_SUITE_NAMESPACE, kafkaConnectResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaBridge() {
        JsonObject kafkaBridgeResource = exampleResources.get(KafkaBridge.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaBridgeResource.toString());
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, kafkaBridgeResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaMirrorMaker2() {
        JsonObject kafkaMirrorMaker2Resource = exampleResources.get(KafkaMirrorMaker2.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaMirrorMaker2Resource.toString()
                .replace("cluster-a-kafka-bootstrap", "my-cluster-kafka-bootstrap")
                .replace("cluster-b-kafka-bootstrap", "my-cluster-kafka-bootstrap"));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(Environment.TEST_SUITE_NAMESPACE, kafkaMirrorMaker2Resource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaRebalance() {
        String cruiseControlClusterName = "cruise-control";
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(Environment.TEST_SUITE_NAMESPACE, cruiseControlClusterName, 3).build());
        JsonObject kafkaRebalanceResource = exampleResources.get(KafkaRebalance.RESOURCE_KIND);
        kafkaRebalanceResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, cruiseControlClusterName);
        cmdKubeClient().applyContent(kafkaRebalanceResource.toString());
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(Environment.TEST_SUITE_NAMESPACE, "my-rebalance", KafkaRebalanceState.PendingProposal);
    }

    @AfterAll
    void teardown() {
        cmdKubeClient().deleteContent(exampleResources.get(KafkaRebalance.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(exampleResources.get(KafkaMirrorMaker2.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(exampleResources.get(KafkaBridge.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(exampleResources.get(KafkaConnect.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(exampleResources.get(KafkaTopic.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(exampleResources.get(KafkaUser.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(exampleResources.get(Kafka.RESOURCE_KIND).toString());
    }
}
