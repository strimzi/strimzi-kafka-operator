/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources;

import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

public class OlmResource {
    private static final Logger LOGGER = LogManager.getLogger(OlmResource.class);

    public static void clusterOperator(String operatorDeploymentName, String namespace) throws Exception {
        File catalogSourceFile = File.createTempFile("catalogsource", ".yaml");
        String catalogSource = TestUtils.getFileAsString(OlmResource.class.getClassLoader().getResource("olm/catalog-source.yaml").getPath());
        TestUtils.writeFile(catalogSourceFile.getAbsolutePath(),
                catalogSource.replace("${OPERATOR_NAMESPACE}", namespace).replace("${OLM_MANIFEST_IMAGE}", Environment.OLM_MANIFESTS_IMAGE_NAME));

        ResourceManager.cmdKubeClient().apply(catalogSourceFile);

        if (!KubeClusterResource.getInstance().getDefaultOlmvNamespace().equals(namespace)) {
            File operatorGroupFile = File.createTempFile("operatorgroup", ".yaml");
            String operatorGroup = TestUtils.getFileAsString(OlmResource.class.getClassLoader().getResource("olm/operator-group.yaml").getPath());
            TestUtils.writeFile(operatorGroupFile.getAbsolutePath(), operatorGroup.replace("\\$\\{OPERATOR_NAMESPACE}", namespace));
            ResourceManager.cmdKubeClient().apply(operatorGroupFile);
        }


        File subscriptionFile = File.createTempFile("subscription", ".yaml");
        String subscription = TestUtils.getFileAsString(OlmResource.class.getClassLoader().getResource("olm/subscription.yaml").getPath());
        TestUtils.writeFile(subscriptionFile.getAbsolutePath(), subscription.replace("${OPERATOR_NAMESPACE}", namespace));
        ResourceManager.cmdKubeClient().apply(subscriptionFile);
        // Make sure that operator will be deleted
        ResourceManager.getPointerResources().push(() -> deleteOlm(operatorDeploymentName, namespace));
        // Wait for operator creation
        waitFor(operatorDeploymentName, namespace, 1);
    }

    private static void deleteOlm(String deploymentName, String namespace) {
        ResourceManager.cmdKubeClient().exec("delete", "subscriptions", "-l", "app=strimzi", "-n", namespace);
        ResourceManager.cmdKubeClient().exec("delete", "operatorgroups", "-l", "app=strimzi", "-n", namespace);
        ResourceManager.cmdKubeClient().exec("delete", "catalogsources", "-l", "app=strimzi", "-n", namespace);
        ResourceManager.cmdKubeClient().exec("delete", "csv", "-l", "app=strimzi", "-n", namespace);
        DeploymentUtils.waitForDeploymentDeletion(deploymentName);
    }

    private static void waitFor(String deploymentName, String namespace, int replicas) {
        LOGGER.info("Waiting for deployment {} in namespace {}", deploymentName, namespace);
        DeploymentUtils.waitForDeploymentReady(deploymentName, replicas);
        LOGGER.info("Deployment {} in namespace {} is ready", deploymentName, namespace);
    }
}
