/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.fabric8.kubernetes.api.model.PodCondition;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

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

    public static void waitForMessagesInKafkaConnectFileSink(String namespaceName, String kafkaConnectPodName, String sinkFileName, int sinkReceivedMsgCount) {
        final String lastReceivedMessageIndex = Integer.toString(sinkReceivedMsgCount - 1);
        LOGGER.info("Waiting for messages to be present in file sink on {}/{}", namespaceName, kafkaConnectPodName);
        TestUtils.waitFor("messages to be present in file sink", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.TIMEOUT_FOR_SEND_RECEIVE_MSG,
            () -> cmdKubeClient(namespaceName).execInPod(Level.TRACE, kafkaConnectPodName, "/bin/bash", "-c", "cat " + sinkFileName).out().contains(lastReceivedMessageIndex),
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

    public static void waitForConnectStatusContainsPlugins(String namespaceName, String clusterName) {
        TestUtils.waitFor(String.join("for Connect: %s/%s contains plugins in its status", namespaceName, clusterName),
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KafkaConnectResource.kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getConnectorPlugins() != null
        );
    }

    /**
     * Waits for the propagation of log level changes within the Kafka Connect configuration and verifies the change across all specified Connect pods.
     * This method first checks if the log level change is reflected in the Kafka Connect REST API's logger settings. Then, it validates
     * that the new log level is being applied in the actual log outputs in last 60 seconds.
     *
     * @param testStorage      The {@link TestStorage} contains test details.
     * @param connectPods      A map of Kafka Connect Pods.
     * @param scraperPodName   The name of the pod used to perform network requests within the cluster.
     * @param connectLogMatch  A {@link Predicate <String>} that tests log entries to confirm the presence of the new log level.
     * @param logLevel         The desired log level (e.g., "INFO", "DEBUG") that the method waits to be propagated to the Kafka Connect
     *                         configuration and reflected in the logs.
     */
    public static void waitForConnectLogLevelChangePropagation(TestStorage testStorage, Map<String, String> connectPods, String scraperPodName, Predicate<String> connectLogMatch, String logLevel) {
        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> cmdKubeClient().namespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName())
                + ":8083/admin/loggers/root").out().contains(logLevel)
        );

        TestUtils.waitFor(String.format("wait till change in Log level %s takes place in actual Pods", logLevel), TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.SAFETY_RECONCILIATION_INTERVAL,
            () -> connectPods.keySet().stream()
                .map(cPod -> StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), cPod, "", "60s"))
                .allMatch(connectLogMatch)
        );
    }
}
