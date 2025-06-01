/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.SuiteDoc;
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
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.operator.specific.OlmResource;
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

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

@SuiteDoc(
    description = @Desc("Provides an abstract base for OLM (Operator Lifecycle Manager) system tests, " +
                        "offering common methods for deploying and verifying Strimzi custom resources " +
                        "within an OLM-managed environment."),
    labels = {
        @Label(TestDocsLabels.OLM)
    }
)
public class OlmAbstractST extends AbstractST {

    /**
     * Deploys an example Kafka custom resource.
     * Waits for the Kafka cluster to be ready.
     */
    void doTestDeployExampleKafka() {
        JsonObject kafkaResource = OlmResource.getExampleResources().get(Kafka.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaResource.toString());
        KafkaUtils.waitForKafkaReady(Environment.TEST_SUITE_NAMESPACE, kafkaResource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaUser custom resource.
     * This method first deploys a Kafka cluster with authorization enabled,
     * then deploys the KafkaUser referencing that cluster.
     * Waits for the KafkaUser to be created.
     */
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
        JsonObject kafkaUserResource = OlmResource.getExampleResources().get(KafkaUser.RESOURCE_KIND);
        kafkaUserResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, userKafkaName);
        cmdKubeClient().applyContent(kafkaUserResource.toString());
        KafkaUserUtils.waitForKafkaUserCreation(Environment.TEST_SUITE_NAMESPACE, kafkaUserResource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaTopic custom resource.
     * Waits for the KafkaTopic to be created.
     */
    void doTestDeployExampleKafkaTopic() {
        JsonObject kafkaTopicResource = OlmResource.getExampleResources().get(KafkaTopic.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaTopicResource.toString());
        KafkaTopicUtils.waitForKafkaTopicCreation(Environment.TEST_SUITE_NAMESPACE, kafkaTopicResource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaConnect custom resource.
     * Waits for the KafkaConnect cluster to be ready.
     */
    void doTestDeployExampleKafkaConnect() {
        JsonObject kafkaConnectResource = OlmResource.getExampleResources().get(KafkaConnect.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaConnectResource.toString());
        KafkaConnectUtils.waitForConnectReady(Environment.TEST_SUITE_NAMESPACE, kafkaConnectResource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaBridge custom resource.
     * Waits for the KafkaBridge to be ready.
     */
    void doTestDeployExampleKafkaBridge() {
        JsonObject kafkaBridgeResource = OlmResource.getExampleResources().get(KafkaBridge.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaBridgeResource.toString());
        KafkaBridgeUtils.waitForKafkaBridgeReady(Environment.TEST_SUITE_NAMESPACE, kafkaBridgeResource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaMirrorMaker2 custom resource.
     * Modifies the bootstrap server configuration to point to the deployed Kafka cluster.
     * Waits for the KafkaMirrorMaker2 cluster to be ready.
     */
    void doTestDeployExampleKafkaMirrorMaker2() {
        JsonObject kafkaMirrorMaker2Resource = OlmResource.getExampleResources().get(KafkaMirrorMaker2.RESOURCE_KIND);
        cmdKubeClient().applyContent(kafkaMirrorMaker2Resource.toString()
                .replace("cluster-a-kafka-bootstrap", "my-cluster-kafka-bootstrap")
                .replace("cluster-b-kafka-bootstrap", "my-cluster-kafka-bootstrap"));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(Environment.TEST_SUITE_NAMESPACE, kafkaMirrorMaker2Resource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaRebalance custom resource.
     * This method first deploys a Kafka cluster with Cruise Control enabled,
     * then deploys the KafkaRebalance referencing that cluster.
     * Waits for the KafkaRebalance to reach the 'PendingProposal' state.
     */
    void doTestDeployExampleKafkaRebalance() {
        String cruiseControlClusterName = "cruise-control";
        resourceManager.createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(Environment.TEST_SUITE_NAMESPACE, cruiseControlClusterName, 3).build());
        JsonObject kafkaRebalanceResource = OlmResource.getExampleResources().get(KafkaRebalance.RESOURCE_KIND);
        kafkaRebalanceResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, cruiseControlClusterName);
        cmdKubeClient().applyContent(kafkaRebalanceResource.toString());
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(Environment.TEST_SUITE_NAMESPACE, "my-rebalance", KafkaRebalanceState.PendingProposal);
    }

    @AfterAll
    void teardown() {
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaRebalance.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaMirrorMaker2.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaBridge.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaConnect.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaTopic.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(KafkaUser.RESOURCE_KIND).toString());
        cmdKubeClient().deleteContent(OlmResource.getExampleResources().get(Kafka.RESOURCE_KIND).toString());
    }
}
