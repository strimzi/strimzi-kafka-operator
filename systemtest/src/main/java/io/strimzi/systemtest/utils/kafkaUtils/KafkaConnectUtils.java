/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.labels.LabelSelectors;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.CrdClients.kafkaConnectClient;

public class KafkaConnectUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectUtils.class);

    private KafkaConnectUtils() {}

    /**
     * Replaces KafkaConnect in specific Namespace based on the edited resource from {@link Consumer}.
     *
     * @param namespaceName     name of the Namespace where the resource should be replaced.
     * @param resourceName      name of the KafkaConnect's name.
     * @param editor            editor containing all the changes that should be done to the resource.
     */
    public static void replace(String namespaceName, String resourceName, Consumer<KafkaConnect> editor) {
        KafkaConnect kafkaConnect = kafkaConnectClient().inNamespace(namespaceName).withName(resourceName).get();
        KubeResourceManager.get().replaceResourceWithRetries(kafkaConnect, editor);
    }

    /**
     * Wait until the given Kafka Connect is in desired state.
     * @param namespaceName Namespace name
     * @param clusterName name of KafkaConnect cluster
     * @param state desired state
     */
    public static boolean waitForConnectStatus(String namespaceName, String clusterName, Enum<?> state) {
        KafkaConnect kafkaConnect = kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get();
        return KubeResourceManager.get().waitResourceCondition(kafkaConnect, ResourceConditions.resourceHasDesiredState(state), ResourceOperation.getTimeoutForResourceReadiness(kafkaConnect.getKind()));
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
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podNamePrefix, "/bin/bash", "-c", "curl -I http://localhost:8083/connectors").out().contains("HTTP/1.1 200 OK\n"));
        LOGGER.info("KafkaConnect API is available");
    }

    public static void waitForMessagesInKafkaConnectFileSink(String namespaceName, String kafkaConnectPodName, String sinkFileName, int sinkReceivedMsgCount) {
        final String lastReceivedMessageIndex = Integer.toString(sinkReceivedMsgCount - 1);
        LOGGER.info("Waiting for messages to be present in file sink on {}/{}", namespaceName, kafkaConnectPodName);
        TestUtils.waitFor("messages to be present in file sink", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.TIMEOUT_FOR_SEND_RECEIVE_MSG,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(kafkaConnectPodName, "/bin/bash", "-c", "cat " + sinkFileName).out().contains(lastReceivedMessageIndex),
            () -> LOGGER.warn(KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(kafkaConnectPodName, "/bin/bash", "-c", "cat " + sinkFileName).out()));
        LOGGER.info("Expected messages are in file sink on {}/{}", namespaceName, kafkaConnectPodName);
    }

    public static void clearFileSinkFile(String namespaceName, String kafkaConnectPodName, String sinkFileName) {
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(kafkaConnectPodName, "/bin/bash", "-c", "truncate -s 0 " + sinkFileName);
    }

    /**
     * Waits until the kafka connect CR config has changed.
     *
     * @param namespaceName     Namespace name
     * @param propertyValue property value in the Kafka Connect CR config
     * @param propertyKey   property key in the Kafka Connect CR config
     * @param clusterName   cluster name
     */
    public static void waitForKafkaConnectConfigChange(String namespaceName, String propertyValue, String propertyKey, String clusterName) {
        LOGGER.info("Waiting for KafkaConnect property: {} -> {} to change", propertyKey, propertyValue);
        TestUtils.waitFor("KafkaConnect property: " + propertyKey + " -> " + propertyValue + " to change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                String propertyValueFromKafkaConnect =  (String) kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getSpec().getConfig().get(propertyKey);
                LOGGER.debug("Property key -> {}, Current property value -> {}", propertyKey, propertyValueFromKafkaConnect);
                LOGGER.debug(propertyValueFromKafkaConnect + " == " + propertyValue);
                return propertyValueFromKafkaConnect.equals(propertyValue);
            });
        LOGGER.info("KafkaConnect property: {} -> {} changed", propertyKey, propertyValue);
    }

    /**
     * Wait for designated Kafka Connect pod condition to happen.
     *
     * @param namespaceName   Namespace name
     * @param conditionReason Regex of condition reason
     * @param clusterName     Cluster name
     * @param timeoutMs       Max wait time in ms
     */
    public static void waitForConnectPodCondition(String namespaceName, String conditionReason, String clusterName, long timeoutMs) {
        LabelSelector labelSelector = LabelSelectors.connectLabelSelector(clusterName, KafkaConnectResources.componentName(clusterName));

        TestUtils.waitFor("KafkaConnect Pod to have condition: " + conditionReason,
            TestConstants.GLOBAL_POLL_INTERVAL, timeoutMs, () -> {
                List<String> connectPods = PodUtils.listPodNames(namespaceName, labelSelector);
                List<PodCondition> conditions = KubeResourceManager.get().kubeClient().getClient().pods().inNamespace(namespaceName).withName(connectPods.get(0)).get().getStatus().getConditions();
                for (PodCondition condition : conditions) {
                    if (condition.getReason().matches(conditionReason)) {
                        return true;
                    }
                }
                return false;
            });
    }

    public static void waitUntilKafkaConnectStatusConditionContainsMessage(String namespaceName, String clusterName, String message) {
        TestUtils.waitFor("KafkaConnect status to contain message: [" + message + "]",
            TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT, () -> {
                List<Condition> conditions = kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getConditions();
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
            () -> kafkaConnectClient().inNamespace(namespaceName).withName(clusterName).get().getStatus().getConnectorPlugins() != null
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
     * @param connectLogMatch  A {@link Predicate} that tests log entries to confirm the presence of the new log level.
     * @param logLevel         The desired log level (e.g., "INFO", "DEBUG") that the method waits to be propagated to the Kafka Connect
     *                         configuration and reflected in the logs.
     */
    public static void waitForConnectLogLevelChangePropagation(TestStorage testStorage, Map<String, String> connectPods, String scraperPodName, Predicate<String> connectLogMatch, String logLevel) {
        LOGGER.info("Waiting for log4j.properties will contain desired settings");
        TestUtils.waitFor("Logger change", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> KubeResourceManager.get().kubeCmdClient().inNamespace(testStorage.getNamespaceName()).execInPod(scraperPodName, "curl", "http://" + KafkaConnectResources.serviceName(testStorage.getClusterName())
                + ":8083/admin/loggers/root").out().contains(logLevel)
        );

        TestUtils.waitFor(String.format("wait till change in Log level %s takes place in actual Pods", logLevel), TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () -> connectPods.keySet().stream()
                .map(cPod -> StUtils.getLogFromPodByTime(testStorage.getNamespaceName(), cPod, "", "60s"))
                .allMatch(connectLogMatch)
        );
    }
}
