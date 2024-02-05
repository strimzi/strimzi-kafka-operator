/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.fabric8.kubernetes.api.model.PodCondition;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        LOGGER.info("Waiting for KafkaConnect API to be available");
        TestUtils.waitFor("KafkaConnect API to be available", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> cmdKubeClient(namespaceName).execInPod(podNamePrefix, "/bin/bash", "-c", "curl -I http://localhost:8083/connectors").out().contains("HTTP/1.1 200 OK\n"));
        LOGGER.info("KafkaConnect API is available");
    }

    public static void waitForMessagesInKafkaConnectFileSink(String namespaceName, String kafkaConnectPodName, String sinkFileName, String message) {
        LOGGER.info("Waiting for messages to be present in file sink on {}/{}", namespaceName, kafkaConnectPodName);
        TestUtils.waitFor("messages to be present in file sink", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.TIMEOUT_FOR_SEND_RECEIVE_MSG,
            () -> cmdKubeClient(namespaceName).execInPod(Level.TRACE, kafkaConnectPodName, "/bin/bash", "-c", "cat " + sinkFileName).out().contains(message),
            () -> LOGGER.warn(cmdKubeClient(namespaceName).execInPod(Level.TRACE, kafkaConnectPodName, "/bin/bash", "-c", "cat " + sinkFileName).out()));
        LOGGER.info("Expected messages are in file sink on {}/{}", namespaceName, kafkaConnectPodName);
    }

    public static void clearFileSinkFile(String namespaceName, String kafkaConnectPodName, String sinkFileName) {
        cmdKubeClient(namespaceName).execInPod(kafkaConnectPodName, "/bin/bash", "-c", "truncate -s 0 " + sinkFileName);
    }

    /**
     *  Waits until the kafka connect CR config has changed.
     * @param propertyKey property key in the Kafka Connect CR config
     * @param propertyValue property value in the Kafka Connect CR config
     * @param namespace Namespace name
     * @param clusterName cluster name
     */
    public static void waitForKafkaConnectConfigChange(String propertyKey, String propertyValue, String namespace, String clusterName) {
        LOGGER.info("Waiting for KafkaConnect property: {} -> {} to change", propertyKey, propertyValue);
        TestUtils.waitFor("KafkaConnect property: " + propertyKey + " -> " + propertyValue + " to change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                String propertyValueFromKafkaConnect =  (String) KafkaConnectResource.kafkaConnectClient().inNamespace(namespace).withName(clusterName).get().getSpec().getConfig().get(propertyKey);
                LOGGER.debug("Property key -> {}, Current property value -> {}", propertyKey, propertyValueFromKafkaConnect);
                LOGGER.debug(propertyValueFromKafkaConnect + " == " + propertyValue);
                return propertyValueFromKafkaConnect.equals(propertyValue);
            });
        LOGGER.info("KafkaConnect property: {} -> {} changed", propertyKey, propertyValue);
    }

    /**
     * Wait for designated Kafka Connect pod condition to happen.
     * @param conditionReason Regex of condition reason
     * @param namespace Namespace name
     * @param clusterName Cluster name
     * @param timeoutMs Max wait time in ms
     */
    public static void waitForConnectPodCondition(String conditionReason, String namespace, String clusterName, long timeoutMs) {
        TestUtils.waitFor("KafkaConnect Pod to have condition: " + conditionReason,
            TestConstants.GLOBAL_POLL_INTERVAL, timeoutMs, () -> {
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
        TestUtils.waitFor("KafkaConnect status to contain message: [" + message + "]",
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT, () -> {
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
     * Waits until the Kafka Connect instance contains all specified connector classes, inferring the expected number of plugins from the list size.
     * @param namespace Namespace name
     * @param clusterName Cluster name
     * @param connectorClasses List of connector class names to check for
     */
    public static void waitForKafkaConnectConnectorClasses(String namespace, String clusterName, List<String> connectorClasses) {
        int expectedPluginCount = connectorClasses.size();
        LOGGER.info("Waiting for Kafka Connect to contain exactly {} plugins and connector classes: {}", expectedPluginCount, connectorClasses);
        TestUtils.waitFor("Kafka Connect to have specific connector classes", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                KafkaConnect kafkaConnect = KafkaConnectResource.kafkaConnectClient().inNamespace(namespace).withName(clusterName).get();
                if (kafkaConnect.getStatus().getConnectorPlugins().size() < expectedPluginCount) {
                    LOGGER.debug("Kafka Connect has fewer than {} plugins", expectedPluginCount);
                    return false;
                }
                boolean allConnectorClassesPresent = connectorClasses.stream().allMatch(connectorClass ->
                        kafkaConnect.getStatus().getConnectorPlugins().stream().anyMatch(connectorPlugin -> connectorPlugin.getConnectorClass().contains(connectorClass))
                );
                if (!allConnectorClassesPresent) {
                    LOGGER.debug("Not all specified connector classes are present");
                    return false;
                }
                return true;
            });
        LOGGER.info("Kafka Connect contains all specified connector classes: {}", connectorClasses);
    }

}
