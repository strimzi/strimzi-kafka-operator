/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.operator.specific.OlmResource;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class OlmAbstractST extends AbstractST {

    void doTestDeployExampleKafka() {
        JsonObject kafkaResource = OlmResource.getExampleResources().get(Kafka.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaResource.toString());
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, kafkaResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaUser() {
        String userKafkaName = "user-kafka";
        // KafkaUser example needs Kafka with authorization
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaPersistent(userKafkaName, 1, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                .endKafka()
            .endSpec()
            .build());
        JsonObject kafkaUserResource = OlmResource.getExampleResources().get(KafkaUser.RESOURCE_KIND);
        kafkaUserResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, userKafkaName);
        cmdKubeClient().applyContent(kafkaUserResource.toString());
        KafkaUserUtils.waitForKafkaUserCreation(Environment.TEST_SUITE_NAMESPACE, kafkaUserResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaTopic() {
        JsonObject kafkaTopicResource = OlmResource.getExampleResources().get(KafkaTopic.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaTopicResource.toString());
        KafkaTopicUtils.waitForKafkaTopicCreation(Environment.TEST_SUITE_NAMESPACE, kafkaTopicResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaConnect() {
        JsonObject kafkaConnectResource = OlmResource.getExampleResources().get(KafkaConnect.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaConnectResource.toString());
        KafkaConnectUtils.waitForConnectReady(Environment.TEST_SUITE_NAMESPACE, kafkaConnectResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaBridge() {
        JsonObject kafkaBridgeResource = OlmResource.getExampleResources().get(KafkaBridge.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaBridgeResource.toString());
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, kafkaBridgeResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaMirrorMaker() {
        JsonObject kafkaMirrorMakerResource = OlmResource.getExampleResources().get(KafkaMirrorMaker.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaMirrorMakerResource.toString()
                .replace("my-source-cluster-kafka-bootstrap", "my-cluster-kafka-bootstrap")
                .replace("my-target-cluster-kafka-bootstrap", "my-cluster-kafka-bootstrap"));
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(Environment.TEST_SUITE_NAMESPACE, kafkaMirrorMakerResource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaMirrorMaker2() {
        JsonObject kafkaMirrorMaker2Resource = OlmResource.getExampleResources().get(KafkaMirrorMaker2.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaMirrorMaker2Resource.toString()
                .replace("cluster-a-kafka-bootstrap", "my-cluster-kafka-bootstrap")
                .replace("cluster-b-kafka-bootstrap", "my-cluster-kafka-bootstrap"));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(Environment.TEST_SUITE_NAMESPACE, kafkaMirrorMaker2Resource.getJsonObject("metadata").getString("name"));
    }

    void doTestDeployExampleKafkaRebalance() {
        String cruiseControlClusterName = "cruise-control";
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(cruiseControlClusterName, 3, 3).build());
        JsonObject kafkaRebalanceResource = OlmResource.getExampleResources().get(KafkaRebalance.RESOURCE_KIND);
        kafkaRebalanceResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, cruiseControlClusterName);
        cmdKubeClient().applyContent(kafkaRebalanceResource.toString());
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(Environment.TEST_SUITE_NAMESPACE, "my-rebalance", KafkaRebalanceState.PendingProposal);
    }

    @AfterAll
    void teardown() {
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaRebalance.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaMirrorMaker2.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaMirrorMaker.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaBridge.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaConnect.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaTopic.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaUser.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(Kafka.RESOURCE_KIND).toString());
    }
}
