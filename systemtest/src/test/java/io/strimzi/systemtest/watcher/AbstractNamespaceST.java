/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClients;
import io.strimzi.systemtest.kafkaclients.internalClients.KafkaClientsBuilder;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMakerTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUtils;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

@IsolatedSuite
public abstract class AbstractNamespaceST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractNamespaceST.class);

    static final String SECOND_NAMESPACE = "second-namespace-test";
    static final String MAIN_NAMESPACE_CLUSTER_NAME = "my-cluster";
    static final String SECOND_CLUSTER_NAME = MAIN_NAMESPACE_CLUSTER_NAME + "-second";

    private static final String TOPIC_EXAMPLES_DIR = Constants.PATH_TO_PACKAGING_EXAMPLES + "/topic/kafka-topic.yaml";

    void checkKafkaInDiffNamespaceThanCO(String clusterName, String namespace) {
        String previousNamespace = cluster.setNamespace(namespace);
        LOGGER.info("Check if Kafka {}/{} is READY", namespace, clusterName);

        KafkaUtils.waitForKafkaReady(namespace, clusterName);

        cluster.setNamespace(previousNamespace);
    }

    void checkMirrorMakerForKafkaInDifNamespaceThanCO(ExtensionContext extensionContext, String sourceClusterName) {
        String kafkaSourceName = sourceClusterName;
        String kafkaTargetName = MAIN_NAMESPACE_CLUSTER_NAME + "-target";

        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(kafkaTargetName, 1, 1).build());
        resourceManager.createResource(extensionContext, KafkaMirrorMakerTemplates.kafkaMirrorMaker(MAIN_NAMESPACE_CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, false).build());

        LOGGER.info("Waiting for creation {}/{}", SECOND_NAMESPACE, MAIN_NAMESPACE_CLUSTER_NAME + "-mirror-maker");
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(SECOND_NAMESPACE, MAIN_NAMESPACE_CLUSTER_NAME);
        cluster.setNamespace(previousNamespace);
    }

    void deployKafkaConnectorWithSink(ExtensionContext extensionContext, String clusterName) {
        final TestStorage testStorage = new TestStorage(extensionContext, SECOND_NAMESPACE);

        // Deploy Kafka Connector
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", TOPIC_NAME);
        connectorConfig.put("file", Constants.DEFAULT_SINK_FILE_PATH);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        resourceManager.createResource(extensionContext, KafkaConnectorTemplates.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .withConfig(connectorConfig)
            .endSpec()
            .build());
        KafkaConnectorUtils.waitForConnectorReady(testStorage.getNamespaceName(), clusterName);

        String kafkaConnectPodName = kubeClient().listPods(clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND).get(0).getMetadata().getName();
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(testStorage.getNamespaceName(), kafkaConnectPodName);

        KafkaClients kafkaClients = new KafkaClientsBuilder()
            .withTopicName(TOPIC_NAME)
            .withMessageCount(testStorage.getMessageCount())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(SECOND_CLUSTER_NAME))
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .build();

        resourceManager.createResource(extensionContext, kafkaClients.producerStrimzi(), kafkaClients.consumerStrimzi());
        ClientUtils.waitForClientsSuccess(testStorage);

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }
}
