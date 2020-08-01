/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;

public class OlmResource {
    private static final Logger LOGGER = LogManager.getLogger(OlmResource.class);

    private static Map<String, JsonObject> exampleResources = new HashMap<>();

    public static void clusterOperator(String namespace) throws Exception {
        clusterOperator(namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    public static void clusterOperator(String namespace, long operationTimeout, long reconciliationInterval) throws IOException {
        if (!KubeClusterResource.getInstance().getDefaultOlmNamespace().equals(namespace)) {
            File operatorGroupFile = File.createTempFile("operatorgroup", ".yaml");

            InputStream groupInputStream = OlmResource.class.getClassLoader().getResourceAsStream("olm/operator-group.yaml");
            String operatorGroup = TestUtils.readResource(groupInputStream);
            TestUtils.writeFile(operatorGroupFile.getAbsolutePath(), operatorGroup.replace("${OPERATOR_NAMESPACE}", namespace));
            ResourceManager.cmdKubeClient().apply(operatorGroupFile);
        }

        String csvName = Environment.OLM_APP_BUNDLE_PREFIX + "." + Environment.OLM_OPERATOR_VERSION;

        File subscriptionFile = File.createTempFile("subscription", ".yaml");
        InputStream subscriptionInputStream = OlmResource.class.getClassLoader().getResourceAsStream("olm/subscription.yaml");
        String subscription = TestUtils.readResource(subscriptionInputStream);
        TestUtils.writeFile(subscriptionFile.getAbsolutePath(),
                subscription.replace("${OPERATOR_NAMESPACE}", namespace)
                .replace("${OLM_OPERATOR_NAME}", Environment.OLM_OPERATOR_NAME)
                .replace("${OLM_SOURCE_NAME}", Environment.OLM_SOURCE_NAME)
                .replace("${OLM_SOURCE_NAMESPACE}", ResourceManager.cmdKubeClient().defaultOlmNamespace())
                .replace("${OLM_APP_BUNDLE_PREFIX}", Environment.OLM_APP_BUNDLE_PREFIX)
                .replace("${OLM_OPERATOR_VERSION}", Environment.OLM_OPERATOR_VERSION)
                .replace("${STRIMZI_FULL_RECONCILIATION_INTERVAL_MS}", Long.toString(reconciliationInterval))
                .replace("${STRIMZI_OPERATION_TIMEOUT_MS}", Long.toString(operationTimeout)));

        ResourceManager.cmdKubeClient().apply(subscriptionFile);
        // Make sure that operator will be deleted
        TestUtils.waitFor("Cluster Operator deployment creation", Constants.GLOBAL_POLL_INTERVAL, CR_CREATION_TIMEOUT,
            () -> ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_NAME) != null);
        String deploymentName = ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_NAME);
        ResourceManager.setCoDeploymentName(deploymentName);
        ResourceManager.getPointerResources().push(() -> deleteOlm(deploymentName, namespace, csvName));
        // Wait for operator creation
        waitFor(deploymentName, namespace, 1);

        exampleResources = parseExamplesFromCsv(csvName, namespace);
    }

    public static void deleteOlm(String deploymentName, String namespace, String csvName) {
        ResourceManager.cmdKubeClient().exec("delete", "subscriptions", "-l", "app=strimzi", "-n", namespace);
        ResourceManager.cmdKubeClient().exec("delete", "operatorgroups", "-l", "app=strimzi", "-n", namespace);
        ResourceManager.cmdKubeClient().exec(false, "delete", "csv", csvName, "-n", namespace);
        DeploymentUtils.waitForDeploymentDeletion(deploymentName);
    }

    private static void waitFor(String deploymentName, String namespace, int replicas) {
        LOGGER.info("Waiting for deployment {} in namespace {}", deploymentName, namespace);
        DeploymentUtils.waitForDeploymentAndPodsReady(deploymentName, replicas);
        LOGGER.info("Deployment {} in namespace {} is ready", deploymentName, namespace);
    }

    private static Map<String, JsonObject> parseExamplesFromCsv(String csvName, String namespace) {
        String csvString = ResourceManager.cmdKubeClient().exec(true, false, "get", "csv", csvName, "-o", "json", "-n", namespace).out();
        JsonObject csv = new JsonObject(csvString);
        String almExamples = csv.getJsonObject("metadata").getJsonObject("annotations").getString("alm-examples");
        JsonArray examples = new JsonArray(almExamples);
        return examples.stream().map(o -> (JsonObject) o).collect(Collectors.toMap(object -> object.getString("kind"), object -> object));
    }

    public static Map<String, JsonObject> getExampleResources() {
        return exampleResources;
    }

    public static void setExampleResources(Map<String, JsonObject> exampleResources) {
        OlmResource.exampleResources = exampleResources;
    }
}
