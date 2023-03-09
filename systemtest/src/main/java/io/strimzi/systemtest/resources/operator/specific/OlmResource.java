/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator.specific;

import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroup;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.operator.configuration.OlmConfiguration;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.OlmUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.resources.ResourceManager.CR_CREATION_TIMEOUT;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class OlmResource implements SpecificResourceType {
    private static Map<String, JsonObject> exampleResources = new HashMap<>();
    private static final Logger LOGGER = LogManager.getLogger(OlmResource.class);
    private OlmConfiguration olmConfiguration;
    private String deploymentName;

    public OlmResource(OlmConfiguration olmConfiguration) {
        this.olmConfiguration = olmConfiguration;
    }

    @Override
    public void create(ExtensionContext extensionContext) {

    }

    public void create() {
        ResourceManager.STORED_RESOURCES.computeIfAbsent(olmConfiguration.getExtensionContext().getDisplayName(), k -> new Stack<>());

        // Apply Operator Group in OLM namespace if the default OLM namespace is missing
        if (!KubeClusterResource.getInstance().getDefaultOlmNamespace().equals(olmConfiguration.getNamespaceName())) {
            createOperatorGroup();
        }

        ResourceManager.STORED_RESOURCES.get(olmConfiguration.getExtensionContext().getDisplayName()).push(new ResourceItem(this::deleteCSV));

        createAndModifySubscription(olmConfiguration);

        // Manual installation needs to be approved with a patch
        if (olmConfiguration.getOlmInstallationStrategy() == OlmInstallationStrategy.Manual) {
            OlmUtils.waitUntilNonUsedInstallPlanIsPresent(olmConfiguration.getNamespaceName());
            approveNotApprovedInstallPlan();
        }

        // Make sure that operator will be created
        TestUtils.waitFor("Cluster Operator deployment creation", Constants.GLOBAL_POLL_INTERVAL, CR_CREATION_TIMEOUT,
            () -> kubeClient(olmConfiguration.getNamespaceName()).getDeploymentNameByPrefix(olmConfiguration.getOlmOperatorDeploymentName()) != null);

        deploymentName = kubeClient(olmConfiguration.getNamespaceName()).getDeploymentNameByPrefix(olmConfiguration.getOlmOperatorDeploymentName());
        ResourceManager.setCoDeploymentName(deploymentName);

        // Wait for operator creation
        DeploymentUtils.waitForDeploymentAndPodsReady(olmConfiguration.getNamespaceName(), deploymentName, 1);

        exampleResources = parseExamplesFromCsv();
    }

    @Override
    public void delete() {

    }

    /**
     * Creates OperatorGroup in specific namespace
     */
    public void createOperatorGroup() {
        OperatorGroup operatorGroup = new OperatorGroupBuilder()
            .editOrNewMetadata()
                .withName("strimzi-group")
                .withNamespace(olmConfiguration.getNamespaceName())
                .withLabels(Collections.singletonMap("app", "strimzi"))
            .endMetadata()
            .editOrNewSpec()
                .withTargetNamespaces(olmConfiguration.getNamespaceName())
            .endSpec()
            .build();

        ResourceManager.getInstance().createResource(olmConfiguration.getExtensionContext(), operatorGroup);
    }

    /**
     * Creates Subscription with spec from OlmConfiguration
     */

    public void createAndModifySubscription(OlmConfiguration olmConfig) {
        Subscription subscription = new SubscriptionBuilder()
            .editOrNewMetadata()
                .withName("strimzi-sub")
                .withNamespace(olmConfig.getNamespaceName())
                .withLabels(Collections.singletonMap("app", "strimzi"))
            .endMetadata()
            .editOrNewSpec()
                .withName(olmConfig.getOlmOperatorName())
                .withSource(olmConfig.getOlmSourceName())
                .withSourceNamespace(olmConfig.getOlmSourceNamespace())
                .withChannel(olmConfig.getChannelName())
                .withStartingCSV(olmConfig.getCsvName())
                .withInstallPlanApproval(olmConfig.getOlmInstallationStrategy().toString())
                .editOrNewConfig()
                    .withEnv(olmConfig.getAllEnvVariablesForOlm())
                .endConfig()
            .endSpec()
            .build();

        ResourceManager.getInstance().createResource(olmConfig.getExtensionContext(), subscription);
    }

    /**
     * Approves non-approved InstallPlan.
     * Used for manual installation type.
     */
    private void approveNotApprovedInstallPlan() {
        String notApprovedIPName = kubeClient().getNonApprovedInstallPlan(olmConfiguration.getNamespaceName()).getMetadata().getName();
        kubeClient().approveInstallPlan(olmConfiguration.getNamespaceName(), notApprovedIPName);
    }

    private Map<String, JsonObject> parseExamplesFromCsv() {
        JsonArray examples = new JsonArray(kubeClient().getCsvWithPrefix(olmConfiguration.getNamespaceName(), olmConfiguration.getOlmAppBundlePrefix()).getMetadata().getAnnotations().get("alm-examples"));
        return examples.stream().map(o -> (JsonObject) o).collect(Collectors.toMap(object -> object.getString("kind"), object -> object));
    }

    public static Map<String, JsonObject> getExampleResources() {
        return exampleResources;
    }

    public OlmConfiguration getOlmConfiguration() {
        return olmConfiguration;
    }

    public void deleteCSV() {
        LOGGER.info("Deleting CSV {}/{}", olmConfiguration.getNamespaceName(), olmConfiguration.getOlmAppBundlePrefix());
        if (olmConfiguration.getOperatorVersion() == null) {
            kubeClient().deleteCsv(olmConfiguration.getNamespaceName(), kubeClient().getCsvWithPrefix(olmConfiguration.getNamespaceName(), olmConfiguration.getOlmAppBundlePrefix()).getMetadata().getName());
        } else {
            kubeClient().deleteCsv(olmConfiguration.getNamespaceName(), olmConfiguration.getCsvName());
        }
    }
}
