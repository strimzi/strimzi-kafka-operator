/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.kafkaclients.internalClients.InternalKafkaClient;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static io.strimzi.systemtest.Constants.CO_OPERATION_TIMEOUT_SHORT;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectUtils.class);

    private KafkaConnectUtils() {}

    /**
     * Wait until the given Kafka Connect is in desired state.
     * @param clusterName name of KafkaConnect cluster
     * @param status desired state
     */
    public static boolean waitForConnectStatus(String clusterName, Enum<?>  status) {
        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(kubeClient().getNamespace()).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(KafkaConnectResource.kafkaConnectClient(), kafkaConnect, status);
    }

    public static boolean waitForConnectReady(String clusterName) {
        return waitForConnectStatus(clusterName, Ready);
    }

    public static void waitForConnectNotReady(String clusterName) {
        waitForConnectStatus(clusterName, NotReady);
    }

    public static void waitUntilKafkaConnectRestApiIsAvailable(String podNamePrefix) {
        LOGGER.info("Waiting until KafkaConnect API is available");
        TestUtils.waitFor("Waiting until KafkaConnect API is available", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> cmdKubeClient().execInPod(podNamePrefix, "/bin/bash", "-c", "curl -I http://localhost:8083/connectors").out().contains("HTTP/1.1 200 OK\n"));
        LOGGER.info("KafkaConnect API is available");
    }

    public static void waitForMessagesInKafkaConnectFileSink(String kafkaConnectPodName, String sinkFileName, String message) {
        LOGGER.info("Waiting for messages in file sink on {}", kafkaConnectPodName);
        TestUtils.waitFor("messages in file sink", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_SEND_RECEIVE_MSG,
            () -> cmdKubeClient().execInPod(kafkaConnectPodName, "/bin/bash", "-c", "cat " + sinkFileName).out().contains(message));
        LOGGER.info("Expected messages are in file sink on {}", kafkaConnectPodName);
    }

    public static void waitForMessagesInKafkaConnectFileSink(String kafkaConnectPodName, String sinkFileName) {
        waitForMessagesInKafkaConnectFileSink(kafkaConnectPodName, sinkFileName,
                "\"Hello-world - 99\"");
    }

    /**
     *  Waits until the kafka connect CR config has changed.
     * @param propertyKey property key in the Kafka Connect CR config
     * @param propertyValue property value in the Kafka Connect CR config
     * @param namespace namespace name
     * @param clusterName cluster name
     */
    public static void waitForKafkaConnectConfigChange(String propertyKey, String propertyValue, String namespace, String clusterName) {
        LOGGER.info("Waiting for Kafka Connect property {} -> {} change", propertyKey, propertyValue);
        TestUtils.waitFor("Waiting for Kafka Connect config " + propertyKey + " -> " + propertyValue, Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
            () -> {
                String propertyValueFromKafkaConnect =  (String) KafkaConnectResource.kafkaConnectClient().inNamespace(namespace).withName(clusterName).get().getSpec().getConfig().get(propertyKey);
                LOGGER.debug("Property key -> {}, Current property value -> {}", propertyKey, propertyValueFromKafkaConnect);
                LOGGER.debug(propertyValueFromKafkaConnect + " == " + propertyValue);
                return propertyValueFromKafkaConnect.equals(propertyValue);
            });
        LOGGER.info("Kafka Connect property {} -> {} change", propertyKey, propertyValue);
    }

    /**
     * Wait for designated Kafka Connect resource condition type and reason to happen.
     * @param conditionReason String regexp of condition reason
     * @param conditionType String regexp of condition type
     * @param namespace namespace name
     * @param clusterName cluster name
     */
    public static void waitForKafkaConnectCondition(String conditionReason, String conditionType, String namespace, String clusterName) {
        TestUtils.waitFor("Wait for KafkaConnect '" + conditionReason + "' condition with type '" + conditionType + "'.",
                Constants.GLOBAL_POLL_INTERVAL, CO_OPERATION_TIMEOUT_SHORT * 2, () -> {
                List<Condition> conditions = KafkaConnectResource.kafkaConnectClient().inNamespace(namespace).withName(clusterName).get().getStatus().getConditions();
                for (Condition condition : conditions) {
                    if (condition.getReason().matches(conditionReason) && condition.getType().matches(conditionType)) {
                        return true;
                    }
                }
                return false;
            });
    }

    public static void waitUntilKafkaConnectStatusConditionContainsMessage(String clusterName, String namespace, String message) {
        TestUtils.waitFor("KafkaConnect Status with message [" + message + "]",
            Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
                List<Condition> conditions = KafkaConnectResource.kafkaConnectClient().inNamespace(namespace).withName(clusterName).get().getStatus().getConditions();
                for (Condition condition : conditions) {
                    if (condition.getMessage().matches(message)) {
                        return true;
                    }
                }
                return false;
            });
    }

    /**
     * Send and receive messages through file sink connector (using Kafka Connect).
     * @param connectPodName kafkaConnect pod name
     * @param topicName topic to be used
     * @param kafkaClientsPodName kafkaClients pod name
     * @param namespace namespace name
     * @param clusterName cluster name
     */
    public static void sendReceiveMessagesThroughConnect(String connectPodName, String topicName, String kafkaClientsPodName, String namespace, String clusterName) {
        LOGGER.info("Send and receive messages through KafkaConnect");
        KafkaConnectUtils.waitUntilKafkaConnectRestApiIsAvailable(connectPodName);
        KafkaConnectorUtils.createFileSinkConnector(kafkaClientsPodName, topicName, Constants.DEFAULT_SINK_FILE_PATH, KafkaConnectResources.url(clusterName, namespace, 8083));

        InternalKafkaClient internalKafkaClient = new InternalKafkaClient.Builder()
                .withUsingPodName(kafkaClientsPodName)
                .withTopicName(topicName)
                .withNamespaceName(namespace)
                .withClusterName(clusterName)
                .withMessageCount(100)
                .withListenerName(Constants.PLAIN_LISTENER_DEFAULT_NAME)
                .build();

        internalKafkaClient.checkProducedAndConsumedMessages(
                internalKafkaClient.sendMessagesPlain(),
                internalKafkaClient.receiveMessagesPlain()
        );
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(connectPodName, Constants.DEFAULT_SINK_FILE_PATH, "99");
    }
}
