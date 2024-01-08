/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.systemtest.resources.crd.KafkaConnectorResource;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestConstants.GLOBAL_RECONCILIATION_COUNT;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;

public class KafkaConnectorUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectorUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(KafkaConnector.RESOURCE_KIND);

    private KafkaConnectorUtils() {}

    /**
     * WaitForStabilityConnector method, verifying stability of connector
     * @param namespaceName Namespace name
     * @param connectorName connector name
     * @param connectPodName Connect pod name
     */
    public static void waitForConnectorStability(String namespaceName, String connectorName, String connectPodName) {
        // alternative to sync hassling AtomicInteger one could use an integer array instead
        // not need to be final because reference to the array does not get another array assigned
        int[] i = {0};

        TestUtils.waitFor("stability of KafkaConnector: " + namespaceName + "/" + connectorName, TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.KAFKA_CONNECTOR_STABILITY_TIMEOUT,
            () -> {
                String availableConnectors = getCreatedConnectors(namespaceName, connectPodName);
                if (availableConnectors.contains(connectorName)) {
                    LOGGER.info("KafkaConnector: {}/{} is present. Must remain stable for: {} second(s)", namespaceName, connectorName,
                            TestConstants.GLOBAL_RECONCILIATION_COUNT - i[0]);
                    return i[0]++ == (TestConstants.GLOBAL_RECONCILIATION_COUNT);
                } else {
                    throw new RuntimeException("KafkaConnector" + namespaceName + "/" + connectorName + " is not stable!");
                }
            }, () -> ResourceManager.logCurrentResourceStatus(KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get())
        );
    }

    /**
     * Wait until KafkaConnector is in desired state
     * @param namespaceName Namespace name
     * @param connectorName name of KafkaConnector
     * @param state desired state
     */
    public static boolean waitForConnectorStatus(String namespaceName, String connectorName, Enum<?>  state) {
        KafkaConnector kafkaConnector = KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get();
        return ResourceManager.waitForResourceStatus(KafkaConnectorResource.kafkaConnectorClient(), kafkaConnector, state);
    }

    public static boolean waitForConnectorReady(String namespaceName, String connectorName) {
        return waitForConnectorStatus(namespaceName, connectorName, Ready);
    }

    public static boolean waitForConnectorNotReady(String namespaceName, String connectorName) {
        return waitForConnectorStatus(namespaceName, connectorName, NotReady);
    }

    public static String getCreatedConnectors(String namespaceName, String connectPodName) {
        return cmdKubeClient(namespaceName).execInPod(connectPodName, "/bin/bash", "-c",
                "curl -X GET http://localhost:8083/connectors"
        ).out();
    }

    public static void waitForConnectorDeletion(String namespaceName, String connectorName) {
        TestUtils.waitFor(connectorName + " connector deletion", TestConstants.GLOBAL_POLL_INTERVAL, READINESS_TIMEOUT, () -> {
            if (KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get() == null) {
                return true;
            } else {
                LOGGER.info("KafkaConnector: {}/{} is not deleted yet, triggering force delete", namespaceName, connectorName);
                cmdKubeClient().deleteByName(KafkaConnector.RESOURCE_KIND, connectorName);
                return false;
            }
        }, () -> ResourceManager.logCurrentResourceStatus(KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get()));
    }

    public static void createFileSinkConnector(String namespaceName, String podName, String topicName, String sinkFileName, String apiUrl) {
        cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "curl -X POST -H \"Content-Type: application/json\" " + "--data '{ \"name\": \"sink-test\", " +
                "\"config\": " + "{ \"connector.class\": \"FileStreamSink\", " +
                "\"tasks.max\": \"1\", \"topics\": \"" + topicName + "\"," + " \"file\": \"" + sinkFileName + "\" } }' " +
                apiUrl + "/connectors"
        );
    }

    public static void waitForConnectorsTaskMaxChange(String namespaceName, String connectorName, int taskMax) {
        TestUtils.waitFor("KafkaConnector taskMax change", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> (KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName)
                .withName(connectorName).get().getSpec().getTasksMax() == taskMax)
                && (KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName)
                .withName(connectorName).get().getStatus().getTasksMax() == taskMax)
        );
    }

    public static void waitForConnectorTaskState(String namespaceName, String connectorName, int taskId, String state) {
        LOGGER.info("Waiting for task with id: {} to be in state {}", taskId, state);
        TestUtils.waitFor("KafkaConnector task status to be: " + state, TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT,
            () -> (getConnectorTaskState(namespaceName, connectorName, taskId).equals(state))
        );
    }

    public static String getConnectorTaskState(String namespaceName, String connectorName, int taskId) {
        KafkaConnectorStatus connectorState = KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get().getStatus();
        Map<String, Object> connectorTask = ((ArrayList<Map<String, Object>>) connectorState.getConnectorStatus().get("tasks")).stream().filter(conn -> conn.get("id").equals(taskId)).collect(Collectors.toList()).get(0);
        return  (String) connectorTask.get("state");
    }

    public static void waitForConnectorAutoRestartCount(String namespaceName, String connectorName, int restartCount) {
        LOGGER.info("Waiting for KafkaConnector: {}/{} to have autoRestartCount: {}", namespaceName, connectorName, restartCount);
        TestUtils.waitFor("KafkaConnector: " + namespaceName + "/" + connectorName + " to have autoRestartCount: " + restartCount, TestConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestConstants.GLOBAL_TIMEOUT,
            () -> (getConnectorAutoRestartCount(namespaceName, connectorName) == restartCount)
        );
    }

    public static int getConnectorAutoRestartCount(String namespaceName, String connectorName) {
        KafkaConnectorStatus connectorState = KafkaConnectorResource.kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get().getStatus();
        // If autoRestartCount is 0 it's not present in the resource status and returns null when looked for
        if (connectorState.getAutoRestart() != null) {
            return connectorState.getAutoRestart().getCount();
        }
        return 0;
    }

    public static void waitForConnectorsTaskMaxChangeViaAPI(String namespaceName, String connectPodName, String connectorName, int taskMax) {
        TestUtils.waitFor("KafkaConnector taskMax change via API", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
            ResourceOperation.getTimeoutForResourceReadiness(KafkaConnector.RESOURCE_KIND),
            () -> getConnectorSpecFromConnectAPI(namespaceName, connectPodName, connectorName).contains("\"tasks.max\":\"" + taskMax + "\""));
    }

    public static String getConnectorSpecFromConnectAPI(String namespaceName, String podName, String connectorName) {
        return cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "curl http://localhost:8083/connectors/" + connectorName).out();
    }

    public static String getConnectorConfig(String namespaceName, String podName, String connectorName, String apiUrl) {
        return cmdKubeClient(namespaceName).execInPod(podName, "/bin/bash", "-c", "curl http://" + apiUrl + ":8083/connectors/" +
            connectorName + "/config").out();
    }

    public static String waitForConnectorConfigUpdate(String namespaceName, String podName, String connectorName, String oldConfig, String apiUrl) {
        TestUtils.waitFor("KafkaConnector config to contain desired values", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS,
            ResourceOperation.getTimeoutForResourceReadiness(KafkaConnector.RESOURCE_KIND),
            () -> !oldConfig.equals(getConnectorConfig(namespaceName, podName, connectorName, apiUrl)));
        return getConnectorConfig(namespaceName, podName, connectorName, apiUrl);
    }

    /**
     * Checks stability of Connector's spec on Connect API, which should be same like before changes
     */
    public static void waitForConnectorSpecFromConnectAPIStability(String namespaceName, String podName, String connectorName, String oldSpec) {
        int[] stableCounter = {0};

        TestUtils.waitFor("KafkaConnector's spec to be stable", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_STATUS_TIMEOUT, () -> {
            if (getConnectorSpecFromConnectAPI(namespaceName, podName, connectorName).equals(oldSpec)) {
                stableCounter[0]++;
                if (stableCounter[0] == TestConstants.GLOBAL_STABILITY_OFFSET_COUNT) {
                    LOGGER.info("Connector's spec is stable for: {} poll intervals", stableCounter[0]);
                    return true;
                }
            } else {
                LOGGER.info("Connector's spec is not stable. Going to set the counter to zero");
                stableCounter[0] = 0;
                return false;
            }
            LOGGER.info("Connector's spec gonna be stable in {} polls", TestConstants.GLOBAL_STABILITY_OFFSET_COUNT - stableCounter[0]);
            return false;
        });
    }

    public static void waitForConnectorWorkerStatus(String namespaceName, String podName, String connectName, String connectorName, String state) {
        LOGGER.info("Waiting for worker of KafkaConnector: {}/{} to be in {} state", namespaceName, connectorName, state);
        TestUtils.waitFor("KafkaConnector's:" + connectorName + " worker to be in state: " + state, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                JsonObject connectorStatus = new JsonObject(
                        cmdKubeClient().namespace(namespaceName).execInPod(podName,
                        "curl", "GET",
                        "http://" + KafkaConnectResources.serviceName(connectName) + ":8083/connectors/" + connectorName + "/status").out().trim()
                );
                return connectorStatus.getJsonObject("connector").getString("state").equals(state);
            }
        );
    }

    public static void loggerStabilityWait(String namespaceName, String connectClusterName, String podName, String desiredLogger, String connectorName) {
        int[] counter = {0};
        TestUtils.waitFor("KafkaConnector logger to be stable", TestConstants.GLOBAL_POLL_INTERVAL, TestConstants.GLOBAL_TIMEOUT,
            () ->  {
                String logger = cmdKubeClient().namespace(namespaceName).execInPod(podName, "curl",
                    "http://" + KafkaConnectResources.serviceName(connectClusterName) + ":8083/admin/loggers/" + connectorName).out();
                if (logger.contains(desiredLogger)) {
                    counter[0]++;
                    LOGGER.info("Logger level is {}. Must remain stable for: {} second(s)", desiredLogger, GLOBAL_RECONCILIATION_COUNT - counter[0]);
                } else {
                    LOGGER.warn("Logger level has changed: {}. Reseting counter from {} to 0", logger, counter[0]);
                    counter[0] = 0;
                }

                if (counter[0] == GLOBAL_RECONCILIATION_COUNT) {
                    LOGGER.info("Logger for connector {} is stable", connectorName);
                    return true;
                }
                return false;
            }
        );
    }
}
