/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.watcher;

import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.systemtest.resources.crd.KafkaMirrorMakerResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectorUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaMirrorMakerUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaTopicUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public abstract class AbstractNamespaceST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(AbstractNamespaceST.class);

    static final String CO_NAMESPACE = "co-namespace-test";
    static final String SECOND_NAMESPACE = "second-namespace-test";
    private static final String TOPIC_EXAMPLES_DIR = TestUtils.USER_PATH + "/../examples/topic/kafka-topic.yaml";

    void checkKafkaInDiffNamespaceThanCO(String clusterName, String namespace) {
        String previousNamespace = cluster.setNamespace(namespace);
        LOGGER.info("Check if Kafka Cluster {} in namespace {}", KafkaResources.kafkaStatefulSetName(clusterName), namespace);

        TestUtils.waitFor("Kafka Cluster status is not in desired state: Ready", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT, () -> {
            Condition kafkaCondition = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get()
                    .getStatus().getConditions().get(0);
            LOGGER.info("Kafka condition status: {}", kafkaCondition.getStatus());
            LOGGER.info("Kafka condition type: {}", kafkaCondition.getType());
            return kafkaCondition.getType().equals(Ready.toString());
        });

        Condition kafkaCondition = KafkaResource.kafkaClient().inNamespace(namespace).withName(clusterName).get()
                .getStatus().getConditions().get(0);

        assertThat(kafkaCondition.getType(), is(Ready.toString()));
        cluster.setNamespace(previousNamespace);
    }

    void checkMirrorMakerForKafkaInDifNamespaceThanCO(String sourceClusterName) {
        String kafkaSourceName = sourceClusterName;
        String kafkaTargetName = CLUSTER_NAME + "-target";

        String previousNamespace = cluster.setNamespace(SECOND_NAMESPACE);
        KafkaResource.kafkaEphemeral(kafkaTargetName, 1, 1).done();
        KafkaMirrorMakerResource.kafkaMirrorMaker(CLUSTER_NAME, kafkaSourceName, kafkaTargetName, "my-group", 1, false).done();

        LOGGER.info("Waiting for creation {} in namespace {}", CLUSTER_NAME + "-mirror-maker", SECOND_NAMESPACE);
        KafkaMirrorMakerUtils.waitForKafkaMirrorMakerReady(CLUSTER_NAME);
        cluster.setNamespace(previousNamespace);
    }

    void deployNewTopic(String topicNamespace, String kafkaClusterNamespace, String topic) {
        LOGGER.info("Creating topic {} in namespace {}", topic, topicNamespace);
        cluster.setNamespace(topicNamespace);
        cmdKubeClient().create(new File(TOPIC_EXAMPLES_DIR));
        KafkaTopicUtils.waitForKafkaTopicReady(topic);
        cluster.setNamespace(kafkaClusterNamespace);
    }

    void deleteNewTopic(String namespace, String topic) {
        LOGGER.info("Deleting topic {} in namespace {}", topic, namespace);
        cluster.setNamespace(namespace);
        cmdKubeClient().deleteByName("KafkaTopic", topic);
        cluster.setNamespace(CO_NAMESPACE);
    }

    void deployKafkaConnectorWithSink(String clusterName, String namespace, String topicName, String connectLabel) {
        // Deploy Kafka Connector
        Map<String, Object> connectorConfig = new HashMap<>();
        connectorConfig.put("topics", topicName);
        connectorConfig.put("file", Constants.DEFAULT_SINK_FILE_PATH);
        connectorConfig.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        connectorConfig.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

        KafkaConnectorResource.kafkaConnector(clusterName)
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .withConfig(connectorConfig)
            .endSpec().done();
        KafkaConnectorUtils.waitForConnectorReady(clusterName);

        String kafkaConnectPodName = kubeClient().listPods(Labels.STRIMZI_KIND_LABEL, connectLabel).get(0).getMetadata().getName();
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(kafkaConnectPodName);

        KafkaClientsResource.deployKafkaClients(false, clusterName + "-" + Constants.KAFKA_CLIENTS).done();

        final String kafkaClientsPodName = kubeClient().listPodsByPrefixInName(clusterName + "-" + Constants.KAFKA_CLIENTS).get(0).getMetadata().getName();

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
            .withUsingPodName(kafkaClientsPodName)
            .withTopicName(topicName)
            .withNamespaceName(namespace)
            .withClusterName(clusterName)
            .withMessageCount(MESSAGE_COUNT)
            .build();

        int sent = internalKafkaClient.sendMessagesPlain();
        assertThat(sent, is(MESSAGE_COUNT));

        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }
}
