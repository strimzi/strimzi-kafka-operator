/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaBridgeUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMaker2Utils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaRebalanceUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.utils.kubeUtils.NamespaceUtils;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.List;
import java.util.Map;

import static io.strimzi.systemtest.resources.CrdClients.kafkaTopicClient;

/**
 * Provides an abstract base for OLM (Operator Lifecycle Manager) system tests
 * offering common methods for deploying and verifying Strimzi custom resources
 * within an OLM-managed environment.
 */
public class OlmAbstractST extends AbstractST {
    // Examples are assigned in respective test classes -> OlmAllNamespaceST and OlmSingleNamespaceST
    Map<String, JsonObject> exampleResources;
    public static final String NAMESPACE = "olm-namespace";

    /**
     * Deploys an example KafkaNodePools CRs - for broker and controller roles.
     */
    void doTestDeployExampleNodePools() {
        JsonObject brokerNodePoolResource = exampleResources.get(KafkaNodePool.RESOURCE_KIND);
        brokerNodePoolResource.getJsonObject("metadata").put("name", "broker");
        brokerNodePoolResource.getJsonObject("spec").getJsonObject("storage").put("type", "ephemeral");
        brokerNodePoolResource.getJsonObject("spec").getJsonObject("storage").remove("volumes");

        // We need to copy it to String because otherwise the `brokerNodePoolResource` will be overridden with controller stuff
        String brokerResourceInString = brokerNodePoolResource.toString();

        JsonObject controllerNodePoolResource = new JsonObject(brokerResourceInString);
        controllerNodePoolResource.getJsonObject("metadata").put("name", "controller");
        controllerNodePoolResource.getJsonObject("spec").put("roles", List.of("controller"));

        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE).applyContent(brokerNodePoolResource.toString());
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE).applyContent(controllerNodePoolResource.toString());
    }

    /**
     * Deploys an example Kafka custom resource.
     * Waits for the Kafka cluster to be ready.
     */
    void doTestDeployExampleKafka() {
        JsonObject kafkaResource = exampleResources.get(Kafka.RESOURCE_KIND);
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE).applyContent(kafkaResource.toString());
        KafkaUtils.waitForKafkaReady(NAMESPACE, kafkaResource.getJsonObject("metadata").getString("name"));
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
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(NAMESPACE, KafkaComponents.getBrokerPoolName(userKafkaName), userKafkaName, 1).build(),
            KafkaNodePoolTemplates.controllerPool(NAMESPACE, KafkaComponents.getControllerPoolName(userKafkaName), userKafkaName, 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafka(NAMESPACE, userKafkaName, 1)
            .editSpec()
                .editKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                .endKafka()
            .endSpec()
            .build());
        JsonObject kafkaUserResource = exampleResources.get(KafkaUser.RESOURCE_KIND);
        kafkaUserResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, userKafkaName);
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE).applyContent(kafkaUserResource.toString());
        KafkaUserUtils.waitForKafkaUserCreation(NAMESPACE, kafkaUserResource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaTopic custom resource.
     * Waits for the KafkaTopic to be created.
     */
    void doTestDeployExampleKafkaTopic() {
        JsonObject kafkaTopicResource = exampleResources.get(KafkaTopic.RESOURCE_KIND);
        String topicName = kafkaTopicResource.getJsonObject("metadata").getString("name");

        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE).applyContent(kafkaTopicResource.toString());
        KafkaTopicUtils.waitForKafkaTopicCreation(NAMESPACE, topicName);
        kafkaTopicClient().inNamespace(NAMESPACE).withName(topicName).delete();
        KafkaTopicUtils.waitForKafkaTopicDeletion(NAMESPACE, topicName);
    }

    /**
     * Deploys an example KafkaConnect custom resource.
     * Waits for the KafkaConnect cluster to be ready.
     */
    void doTestDeployExampleKafkaConnect() {
        JsonObject kafkaConnectResource = exampleResources.get(KafkaConnect.RESOURCE_KIND);
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE).applyContent(kafkaConnectResource.toString());
        KafkaConnectUtils.waitForConnectReady(NAMESPACE, kafkaConnectResource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaBridge custom resource.
     * Waits for the KafkaBridge to be ready.
     */
    void doTestDeployExampleKafkaBridge() {
        JsonObject kafkaBridgeResource = exampleResources.get(KafkaBridge.RESOURCE_KIND);
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE).applyContent(kafkaBridgeResource.toString());
        KafkaBridgeUtils.waitForKafkaBridgeReady(NAMESPACE, kafkaBridgeResource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaMirrorMaker2 custom resource.
     * Modifies the bootstrap server configuration to point to the deployed Kafka cluster.
     * Waits for the KafkaMirrorMaker2 cluster to be ready.
     */
    void doTestDeployExampleKafkaMirrorMaker2() {
        JsonObject kafkaMirrorMaker2Resource = exampleResources.get(KafkaMirrorMaker2.RESOURCE_KIND);
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE).applyContent(kafkaMirrorMaker2Resource.toString()
                .replace("cluster-a-kafka-bootstrap", "my-cluster-kafka-bootstrap")
                .replace("cluster-b-kafka-bootstrap", "my-cluster-kafka-bootstrap"));
        KafkaMirrorMaker2Utils.waitForKafkaMirrorMaker2Ready(NAMESPACE, kafkaMirrorMaker2Resource.getJsonObject("metadata").getString("name"));
    }

    /**
     * Deploys an example KafkaRebalance custom resource.
     * This method first deploys a Kafka cluster with Cruise Control enabled,
     * then deploys the KafkaRebalance referencing that cluster.
     * Waits for the KafkaRebalance to reach the 'PendingProposal' state.
     */
    void doTestDeployExampleKafkaRebalance() {
        String cruiseControlClusterName = "cruise-control";
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPool(NAMESPACE, KafkaComponents.getBrokerPoolName(cruiseControlClusterName), cruiseControlClusterName, 3).build(),
            KafkaNodePoolTemplates.controllerPool(NAMESPACE, KafkaComponents.getControllerPoolName(cruiseControlClusterName), cruiseControlClusterName, 3).build()
        );
        KubeResourceManager.get().createResourceWithWait(KafkaTemplates.kafkaWithCruiseControl(NAMESPACE, cruiseControlClusterName, 3).build());
        JsonObject kafkaRebalanceResource = exampleResources.get(KafkaRebalance.RESOURCE_KIND);
        kafkaRebalanceResource.getJsonObject("metadata").getJsonObject("labels").put(Labels.STRIMZI_CLUSTER_LABEL, cruiseControlClusterName);
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE).applyContent(kafkaRebalanceResource.toString());
        KafkaRebalanceUtils.waitForKafkaRebalanceCustomResourceState(NAMESPACE, "my-rebalance", KafkaRebalanceState.PendingProposal);
    }

    @BeforeAll
    void createNamespace() {
        NamespaceUtils.createNamespaceAndPrepare(NAMESPACE);
    }

    @AfterAll
    void teardown() {
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE)
            .withTimeout(TestConstants.GLOBAL_TIMEOUT_SHORT).deleteContent(exampleResources.get(KafkaRebalance.RESOURCE_KIND).toString());
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE)
            .withTimeout(TestConstants.GLOBAL_TIMEOUT_SHORT).deleteContent(exampleResources.get(KafkaMirrorMaker2.RESOURCE_KIND).toString());
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE)
            .withTimeout(TestConstants.GLOBAL_TIMEOUT_SHORT).deleteContent(exampleResources.get(KafkaBridge.RESOURCE_KIND).toString());
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE)
            .withTimeout(TestConstants.GLOBAL_TIMEOUT_SHORT).deleteContent(exampleResources.get(KafkaConnect.RESOURCE_KIND).toString());
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE)
            .withTimeout(TestConstants.GLOBAL_TIMEOUT_SHORT).deleteContent(exampleResources.get(KafkaTopic.RESOURCE_KIND).toString());
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE)
            .withTimeout(TestConstants.GLOBAL_TIMEOUT_SHORT).deleteContent(exampleResources.get(KafkaUser.RESOURCE_KIND).toString());
        KubeResourceManager.get().kubeCmdClient().inNamespace(NAMESPACE)
            .withTimeout(TestConstants.GLOBAL_TIMEOUT_SHORT).deleteContent(exampleResources.get(Kafka.RESOURCE_KIND).toString());
    }
}
