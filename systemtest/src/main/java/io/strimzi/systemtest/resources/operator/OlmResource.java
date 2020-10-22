/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class OlmResource {
    private static final Logger LOGGER = LogManager.getLogger(OlmResource.class);

    private static final String CO_POD_PREFIX_NAME = "strimzi-cluster-operator-";

    // 3 only three versions
    private static Map<String, Boolean> closedMapInstallPlan = new HashMap<>(3);
    private static List<String> closedListInstallPlans = new ArrayList<>(2);

    private static Map<String, JsonObject> exampleResources = new HashMap<>();

    public static void clusterOperator(String namespace) {
        clusterOperator(namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL, OlmInstallationStrategy.Automatic, true);
    }

    public static void clusterOperator(String namespace, OlmInstallationStrategy olmInstallationStrategy, boolean isLatest) {
        clusterOperator(namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL,
            olmInstallationStrategy, isLatest);
    }

    public static void clusterOperator(String namespace, long operationTimeout, long reconciliationInterval) {
        clusterOperator(namespace, operationTimeout, reconciliationInterval, OlmInstallationStrategy.Automatic, true);
    }

    public static void clusterOperator(String namespace, long operationTimeout, long reconciliationInterval,
                                       OlmInstallationStrategy olmInstallationStrategy, boolean isLatest) {

        // if on cluster is not defaultOlmNamespace apply 'operator group' in current namespace
        if (!KubeClusterResource.getInstance().getDefaultOlmNamespace().equals(namespace)) {
            createOperatorGroup(namespace);
        }

        String csvName;

        if (isLatest) {
            createAndModifySubscriptionLatestRelease(namespace, operationTimeout, reconciliationInterval, olmInstallationStrategy);
            csvName = Environment.OLM_APP_BUNDLE_PREFIX + "." + Environment.OLM_OPERATOR_LATEST_RELEASE_VERSION;
        } else {
            createAndModifySubscriptionPreviousRelease(namespace, operationTimeout, reconciliationInterval, olmInstallationStrategy);
            csvName = Environment.OLM_APP_BUNDLE_PREFIX + "." + Environment.OLM_OPERATOR_PREVIOUS_RELEASE_VERSION;
        }

        // manual installation needs approval with patch
        if (olmInstallationStrategy == OlmInstallationStrategy.Manual) {
            obtainInstallPlanName();
            approveInstallation();
        }

        // Make sure that operator will be deleted
        TestUtils.waitFor("Cluster Operator deployment creation", Constants.GLOBAL_POLL_INTERVAL, CR_CREATION_TIMEOUT,
            () -> ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME) != null);

        String deploymentName = ResourceManager.kubeClient().getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME);
        ResourceManager.setCoDeploymentName(deploymentName);


        ResourceManager.getPointerResources().push(() -> deleteOlm(deploymentName, namespace, csvName));
        // Wait for operator creation
        waitFor(deploymentName, namespace, 1);

        exampleResources = parseExamplesFromCsv(csvName, namespace);
    }

    public static void obtainInstallPlanName() {
        String installPlansPureString = cmdKubeClient().exec("get", "installplan").out();
        String[] installPlansLines = installPlansPureString.split("\n");

        for (String line : installPlansLines) {
            // line NAME  CSV  APPROVAL   APPROVED
            String[] wholeLine = line.split(" ");

            // name
            if (wholeLine[0].startsWith("install-")) {

                // if is not already applied add to closed map
                if (!closedMapInstallPlan.entrySet().contains(wholeLine[0])) {
                    closedMapInstallPlan.put(wholeLine[0], Boolean.FALSE);
                    break;
                }
            }
        }
        if (!(closedMapInstallPlan.keySet().size() > 0)) {
            throw new RuntimeException("No install plans located in namespace:" + cmdKubeClient().namespace());
        }
    }

    public static boolean isUpgradeable() {
        String[] nonUsedInstallPlan = new String[1];

        closedMapInstallPlan.forEach((key, value) -> {
            nonUsedInstallPlan[0] = value == Boolean.FALSE ? key : "";
        });

        return nonUsedInstallPlan[0].equals("");
    }

    public static String getNonUsedInstallPlan() {

        String[] nonUsedInstallPlan = new String[1];

        closedMapInstallPlan.forEach((key, value) -> {
            nonUsedInstallPlan[0] = value == Boolean.FALSE ? key : "";
        });

        return nonUsedInstallPlan[0];
    }

    private static void approveInstallation() {
        try {
            String dynamicScriptContent =
                "#!/bin/bash\n" +
                    cmdKubeClient().cmd() +
                    " patch installplan " + getNonUsedInstallPlan() + " --type json  --patch '[{\"op\": \"add\", \"path\": \"/spec/approved\", \"value\": true}]'";

            InputStream inputStream = new ByteArrayInputStream(dynamicScriptContent.getBytes());
            File patchScript = File.createTempFile("installplan_patch",  ".sh");
            Files.copy(inputStream, patchScript.toPath(), StandardCopyOption.REPLACE_EXISTING);

            Exec.exec("bash", patchScript.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void upgradeClusterOperator() {
        // this is highest version... can not do..

        // CO is running
        if (kubeClient().listPodsByPrefixInName(CO_POD_PREFIX_NAME).get(0) == null) {
            throw new RuntimeException("We can not perform upgrade! Cluster operator pod is not present.");
        }

        obtainInstallPlanName();
        approveInstallation();
    }

    /**
     * Creates OperatorGroup from `olm/operator-group.yaml` and modify "${OPERATOR_NAMESPACE}" attribute in YAML
     * @param namespace namespace where you want to apply OperatorGroup  kind
     */
    private static void createOperatorGroup(String namespace) {
        try {
            File operatorGroupFile = File.createTempFile("operatorgroup", ".yaml");
            InputStream groupInputStream = OlmResource.class.getClassLoader().getResourceAsStream("olm/operator-group.yaml");
            String operatorGroup = TestUtils.readResource(groupInputStream);
            TestUtils.writeFile(operatorGroupFile.getAbsolutePath(), operatorGroup.replace("${OPERATOR_NAMESPACE}", namespace));
            ResourceManager.cmdKubeClient().apply(operatorGroupFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates Subscription from "olm/subscription.yaml" and modify "${OPERATOR_NAMESPACE}", "${OLM_OPERATOR_NAME}...
     * attributes.
     * @param namespace namespace where you want to apply Subscription kind
     * @param reconciliationInterval reconciliation interval of cluster operator
     * @param operationTimeout operation timeout  of cluster operator
     * @param installationStrategy type of installation
     */
    private static void createAndModifySubscription(String namespace, long reconciliationInterval, long operationTimeout,
                                                    OlmInstallationStrategy installationStrategy, String version) {
        try {
            File subscriptionFile = File.createTempFile("subscription", ".yaml");
            InputStream subscriptionInputStream = OlmResource.class.getClassLoader().getResourceAsStream("olm/subscription.yaml");
            String subscription = TestUtils.readResource(subscriptionInputStream);
            TestUtils.writeFile(subscriptionFile.getAbsolutePath(),
                subscription.replace("${OPERATOR_NAMESPACE}", namespace)
                    .replace("${OLM_OPERATOR_NAME}", Environment.OLM_OPERATOR_NAME)
                    .replace("${OLM_SOURCE_NAME}", Environment.OLM_SOURCE_NAME)
                    .replace("${OLM_SOURCE_NAMESPACE}", ResourceManager.cmdKubeClient().defaultOlmNamespace())
                    .replace("${OLM_APP_BUNDLE_PREFIX}", Environment.OLM_APP_BUNDLE_PREFIX)
                    .replace("${OLM_OPERATOR_VERSION}", version)
                    .replace("${OLM_INSTALL_PLAN_APPROVAL}", installationStrategy.toString())
                    .replace("${STRIMZI_FULL_RECONCILIATION_INTERVAL_MS}", Long.toString(reconciliationInterval))
                    .replace("${STRIMZI_OPERATION_TIMEOUT_MS}", Long.toString(operationTimeout)));

            ResourceManager.cmdKubeClient().apply(subscriptionFile);
        }  catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createAndModifySubscriptionLatestRelease(String namespace, long reconciliationInterval,
                                                                 long operationTimeout, OlmInstallationStrategy installationStrategy) {
        createAndModifySubscription(namespace, reconciliationInterval, operationTimeout, installationStrategy,
            Environment.OLM_OPERATOR_LATEST_RELEASE_VERSION);
    }

    private static void createAndModifySubscriptionPreviousRelease(String namespace, long reconciliationInterval,
                                                                  long operationTimeout, OlmInstallationStrategy installationStrategy) {
        createAndModifySubscription(namespace, reconciliationInterval, operationTimeout, installationStrategy,
            Environment.OLM_OPERATOR_PREVIOUS_RELEASE_VERSION);
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
