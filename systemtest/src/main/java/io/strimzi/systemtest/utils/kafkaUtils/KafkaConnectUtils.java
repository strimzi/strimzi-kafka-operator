/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.fabric8.kubernetes.api.model.PodCondition;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;

import java.util.List;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class KafkaConnectUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectUtils.class);

    private KafkaConnectUtils() {}

    /**
     * Wait until the given Kafka Connect is in desired state.
     * @param namespaceName Namespace name
     * @param clusterName name of KafkaConnect cluster
     * @param status desired state
     */
    public static boolean waitForConnectStatus(String namespaceName, String clusterName, Enum<?>  status) {
        KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get();
        return ResourceManager.waitForResourceStatus(KafkaConnectResource.kafkaConnectClient(),
            kafkaConnect.getKind(), namespaceName, kafkaConnect.getMetadata().getName(), status, ResourceOperation.getTimeoutForResourceReadiness(kafkaConnect.getKind()));
    }

    public static boolean waitForConnectReady(String namespaceName, String clusterName) {
        return waitForConnectStatus(namespaceName, clusterName, Ready);
    }

    public static void waitForConnectNotReady(String namespaceName, String clusterName) {
        waitForConnectStatus(namespaceName, clusterName, NotReady);
    }

    public static void waitUntilKafkaConnectRestApiIsAvailable(String namespaceName, String podNamePrefix) {
        LOGGER.info("Waiting until KafkaConnect API is available");
        TestUtils.waitFor("Waiting until KafkaConnect API is available", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_STATUS_TIMEOUT,
            () -> cmdKubeClient(namespaceName).execInPod(podNamePrefix, "/bin/bash", "-c", "curl -I http://localhost:8083/connectors").out().contains("HTTP/1.1 200 OK\n"));
        LOGGER.info("KafkaConnect API is available");
    }

    public static void waitForMessagesInKafkaConnectFileSink(String namespaceName, String kafkaConnectPodName, String sinkFileName, String message) {
        LOGGER.info("Waiting for messages in file sink on {}", kafkaConnectPodName);
        TestUtils.waitFor("messages in file sink", Constants.GLOBAL_POLL_INTERVAL, Constants.TIMEOUT_FOR_SEND_RECEIVE_MSG,
            () -> cmdKubeClient(namespaceName).execInPod(Level.TRACE, kafkaConnectPodName, "/bin/bash", "-c", "cat " + sinkFileName).out().contains(message));
        LOGGER.info("Expected messages are in file sink on {}", kafkaConnectPodName);
    }

    public static void clearFileSinkFile(String namespaceName, String kafkaConnectPodName, String sinkFileName) {
        cmdKubeClient(namespaceName).execInPod(kafkaConnectPodName, "/bin/bash", "-c", "truncate -s 0 " + sinkFileName);
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
     * Wait for designated Kafka Connect pod condition to happen.
     * @param conditionReason Regex of condition reason
     * @param namespace Namespace name
     * @param clusterName Cluster name
     * @param timeoutMs Max wait time in ms
     */
    public static void waitForConnectPodCondition(String conditionReason, String namespace, String clusterName, long timeoutMs) {
        TestUtils.waitFor("Wait for Pod '" + conditionReason + "' condition.",
                Constants.GLOBAL_POLL_INTERVAL, timeoutMs, () -> {
                List<String> connectPods = kubeClient().listPodNames(namespace, clusterName, Labels.STRIMZI_KIND_LABEL, KafkaConnect.RESOURCE_KIND);
                List<PodCondition> conditions = kubeClient().getPod(namespace, connectPods.get(0)).getStatus().getConditions();
                for (PodCondition condition : conditions) {
                    if (condition.getReason().matches(conditionReason)) {
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
}
