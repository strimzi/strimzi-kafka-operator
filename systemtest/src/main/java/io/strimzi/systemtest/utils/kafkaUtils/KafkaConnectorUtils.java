/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kafkaUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorStatus;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceConditions;
import io.strimzi.systemtest.resources.ResourceOperation;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.TestConstants.GLOBAL_STABILIZATION_TIME;
import static io.strimzi.systemtest.enums.CustomResourceStatus.NotReady;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.systemtest.resources.CrdClients.kafkaConnectorClient;

public class KafkaConnectorUtils {

    private static final Logger LOGGER = LogManager.getLogger(KafkaConnectorUtils.class);
    private static final long READINESS_TIMEOUT = ResourceOperation.getTimeoutForResourceReadiness(KafkaConnector.RESOURCE_KIND);

    private KafkaConnectorUtils() {}

    /**
     * Replaces KafkaConnector in specific Namespace based on the edited resource from {@link Consumer}.
     *
     * @param namespaceName     name of the Namespace where the resource should be replaced.
     * @param resourceName      name of the KafkaConnector's name.
     * @param editor            editor containing all the changes that should be done to the resource.
     */
    public static void replace(String namespaceName, String resourceName, Consumer<KafkaConnector> editor) {
        KafkaConnector kafkaConnector = kafkaConnectorClient().inNamespace(namespaceName).withName(resourceName).get();
        KubeResourceManager.get().replaceResourceWithRetries(kafkaConnector, editor);
    }

    /**
     * Wait until KafkaConnector is in desired state
     * @param namespaceName Namespace name
     * @param connectorName name of KafkaConnector
     * @param state desired state
     */
    public static void waitForConnectorStatus(String namespaceName, String connectorName, Enum<?>  state) {
        KafkaConnector kafkaConnector = kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get();
        KubeResourceManager.get().waitResourceCondition(kafkaConnector, ResourceConditions.resourceHasDesiredState(state), ResourceOperation.getTimeoutForResourceReadiness(kafkaConnector.getKind()));
    }

    public static void waitForConnectorReady(String namespaceName, String connectorName) {
        waitForConnectorStatus(namespaceName, connectorName, Ready);
    }

    public static void waitForConnectorNotReady(String namespaceName, String connectorName) {
        waitForConnectorStatus(namespaceName, connectorName, NotReady);
    }

    public static void waitForConnectorDeletion(String namespaceName, String connectorName) {
        TestUtils.waitFor(connectorName + " connector deletion", TestConstants.GLOBAL_POLL_INTERVAL, READINESS_TIMEOUT, () -> {
            if (kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get() == null) {
                return true;
            } else {
                LOGGER.info("KafkaConnector: {}/{} is not deleted yet, triggering force delete", namespaceName, connectorName);
                KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).deleteByName(KafkaConnector.RESOURCE_KIND, connectorName);
                return false;
            }
        });
    }

    public static void createFileSinkConnector(String namespaceName, String podName, String topicName, String sinkFileName, String apiUrl) {
        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "curl -X POST -H \"Content-Type: application/json\" " + "--data '{ \"name\": \"sink-test\", " +
                "\"config\": " + "{ \"connector.class\": \"FileStreamSink\", " +
                "\"tasks.max\": \"1\", \"topics\": \"" + topicName + "\"," + " \"file\": \"" + sinkFileName + "\" } }' " +
                apiUrl + "/connectors"
        );
    }

    public static void waitForConnectorsTaskMaxChange(String namespaceName, String connectorName, int taskMax) {
        TestUtils.waitFor("KafkaConnector taskMax change", TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> (kafkaConnectorClient().inNamespace(namespaceName)
                .withName(connectorName).get().getSpec().getTasksMax() == taskMax)
                && (kafkaConnectorClient().inNamespace(namespaceName)
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
        KafkaConnectorStatus connectorState = kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get().getStatus();
        @SuppressWarnings("unchecked")
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
        KafkaConnectorStatus connectorState = kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get().getStatus();
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
        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "/bin/bash", "-c",
            "curl http://localhost:8083/connectors/" + connectorName).out();
    }

    public static String getConnectorConfig(String namespaceName, String podName, String connectorName, String apiUrl) {
        return KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "/bin/bash", "-c", "curl http://" + apiUrl + ":8083/connectors/" +
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
                if (stableCounter[0] == TestConstants.GLOBAL_STABILITY_OFFSET_TIME) {
                    LOGGER.info("Connector's spec is stable for: {} poll intervals", stableCounter[0]);
                    return true;
                }
            } else {
                LOGGER.info("Connector's spec is not stable. Going to set the counter to zero");
                stableCounter[0] = 0;
                return false;
            }
            LOGGER.info("Connector's spec gonna be stable in {} polls", TestConstants.GLOBAL_STABILITY_OFFSET_TIME - stableCounter[0]);
            return false;
        });
    }

    public static void waitForConnectorWorkerStatus(String namespaceName, String podName, String connectName, String connectorName, String state) {
        LOGGER.info("Waiting for worker of KafkaConnector: {}/{} to be in {} state", namespaceName, connectorName, state);
        TestUtils.waitFor("KafkaConnector's:" + connectorName + " worker to be in state: " + state, TestConstants.POLL_INTERVAL_FOR_RESOURCE_READINESS, TestConstants.GLOBAL_TIMEOUT,
            () -> {
                JsonObject connectorStatus = new JsonObject(
                        KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName,
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
                String logger = KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(podName, "curl",
                    "http://" + KafkaConnectResources.serviceName(connectClusterName) + ":8083/admin/loggers/" + connectorName).out();
                if (logger.contains(desiredLogger)) {
                    counter[0]++;
                    LOGGER.info("Logger level is {}. Must remain stable for: {} second(s)", desiredLogger, GLOBAL_STABILIZATION_TIME - counter[0]);
                } else {
                    LOGGER.warn("Logger level has changed: {}. Reseting counter from {} to 0", logger, counter[0]);
                    counter[0] = 0;
                }

                if (counter[0] == GLOBAL_STABILIZATION_TIME) {
                    LOGGER.info("Logger for connector {} is stable", connectorName);
                    return true;
                }
                return false;
            }
        );
    }

    /**
     * Waits for a removal of the annotation from the KafkaConnector resource.
     *
     * @param namespaceName     name of the Namespace where the KafkaConnector resource is present
     * @param connectorName     name of the KafkaConnector which should be checked
     * @param annotationName    name of the annotation that should be deleted from the KafkaConnector's metadata
     */
    public static void waitForRemovalOfTheAnnotation(String namespaceName, String connectorName, String annotationName) {
        LOGGER.info("Waiting for annotation: {} to be removed from KafkaConnector: {}/{}", annotationName, namespaceName, connectorName);

        TestUtils.waitFor(String.format("annotation %s to be removed from KafkaConnector: %s/%s", annotationName, namespaceName, connectorName),
            TestConstants.GLOBAL_POLL_INTERVAL_5_SECS,
            TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> kafkaConnectorClient().inNamespace(namespaceName).withName(connectorName).get().getMetadata().getAnnotations().get(annotationName) == null
        );
    }

    /**
     * Using Connect API it collects the offsets from the /offset endpoint of the particular connector.
     * It returns the result in JsonNode object for easier handling in the particular tests.
     *
     * @param namespaceName     name of the Namespace where the scraper Pod and Connect are running
     * @param scraperPodName    name of the scraper Pod name for execution of the cURL command
     * @param serviceName       name of the service which exposes the 8083 port
     * @param connectorName     name of the connector that should be checked for the offsets
     * @return  JsonNode object with the offsets (the result of the API call)
     * @throws JsonProcessingException  when the JsonNode object cannot be processed
     */
    public static JsonNode getOffsetOfConnectorFromConnectAPI(
        String namespaceName,
        String scraperPodName,
        String serviceName,
        String connectorName
    ) throws JsonProcessingException {
        final ObjectMapper mapper = new ObjectMapper();

        return mapper.readTree(KubeResourceManager.get().kubeCmdClient().inNamespace(namespaceName).execInPod(scraperPodName,
                "curl", "-X", "GET",
                "http://" + serviceName + ":8083/connectors/" + connectorName + "/offsets").out().trim());
    }

    /**
     * Waits for a specific offset to be present in the File Sink Connector.
     *
     * @param namespaceName     name of the Namespace where the scraper Pod and Connect are running
     * @param scraperPodName    name of the scraper Pod name for execution of the cURL command
     * @param connectName       name of the KafkaConnect resource which should be used for building the service name
     * @param connectorName     name of the connector that should be checked for the offsets
     * @param expectedOffset    offset for which we should wait
     */
    public static void waitForOffsetInFileSinkConnector(
        String namespaceName,
        String scraperPodName,
        String connectName,
        String connectorName,
        int expectedOffset
    ) {
        waitForOffsetInConnector(
            namespaceName,
            scraperPodName,
            KafkaConnectResources.serviceName(connectName),
            connectorName,
            "/offsets/0/offset/kafka_offset",
            expectedOffset
        );
    }

    /**
     * Waits for a specific offset to be present in the Connector.
     *
     * @param namespaceName     name of the Namespace where the scraper Pod and Connect are running
     * @param scraperPodName    name of the scraper Pod name for execution of the cURL command
     * @param serviceName       name of the service which exposes the 8083 port
     * @param connectorName     name of the connector that should be checked for the offsets
     * @param expectedOffset    offset for which we should wait
     */
    public static void waitForOffsetInConnector(
        String namespaceName,
        String scraperPodName,
        String serviceName,
        String connectorName,
        String pathInJsonToOffsetObject,
        int expectedOffset
    ) {
        TestUtils.waitFor("offset on the Connector will contain expected number",
            TestConstants.GLOBAL_POLL_INTERVAL_5_SECS,
            TestConstants.GLOBAL_STATUS_TIMEOUT,
            () -> {
                try {
                    JsonNode offsets = getOffsetOfConnectorFromConnectAPI(namespaceName, scraperPodName, serviceName, connectorName);
                    return offsets.at(pathInJsonToOffsetObject).asInt() == expectedOffset;
                } catch (Exception e) {
                    return false;
                }
            });
    }
}
