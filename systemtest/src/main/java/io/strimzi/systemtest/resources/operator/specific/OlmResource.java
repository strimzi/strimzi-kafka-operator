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
import io.strimzi.systemtest.Environment;
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
        clusterOperator();
    }

    @Override
    public void delete() {

    }

    private void clusterOperator() {
        // if on cluster is not defaultOlmNamespace apply 'operator group' in current namespace
        if (!KubeClusterResource.getInstance().getDefaultOlmNamespace().equals(olmConfiguration.getNamespaceName())) {
            createOperatorGroup();
        }

        ResourceManager.STORED_RESOURCES.get(olmConfiguration.getExtensionContext().getDisplayName()).push(new ResourceItem(this::deleteCSV));

        createAndModifySubscription();

        // manual installation needs approval with patch
        if (olmConfiguration.getOlmInstallationStrategy() == OlmInstallationStrategy.Manual) {
            OlmUtils.waitUntilNonUsedInstallPlanIsPresent(olmConfiguration.getNamespaceName(), olmConfiguration.getOperatorVersion());
            approveNotApprovedInstallPlan();
        }

        // Make sure that operator will be created
        TestUtils.waitFor("Cluster Operator deployment creation", Constants.GLOBAL_POLL_INTERVAL, CR_CREATION_TIMEOUT,
            () -> kubeClient(olmConfiguration.getNamespaceName()).getDeploymentNameByPrefix(olmConfiguration.getOlmOperatorDeploymentName()) != null);

        deploymentName = kubeClient(olmConfiguration.getNamespaceName()).getDeploymentNameByPrefix(Environment.OLM_OPERATOR_DEPLOYMENT_NAME);
        ResourceManager.setCoDeploymentName(deploymentName);

        // Wait for operator creation
        DeploymentUtils.waitForDeploymentAndPodsReady(olmConfiguration.getNamespaceName(), deploymentName, 1);

        exampleResources = parseExamplesFromCsv();
    }

    /**
     * Creates OperatorGroup in specific namespace
     */
    private void createOperatorGroup() {
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
    private void createAndModifySubscription() {
        Subscription subscription = new SubscriptionBuilder()
            .editOrNewMetadata()
                .withName("strimzi-sub")
                .withNamespace(olmConfiguration.getNamespaceName())
                .withLabels(Collections.singletonMap("app", "strimzi"))
            .endMetadata()
            .editOrNewSpec()
                .withName(olmConfiguration.getOlmOperatorName())
                .withSource(olmConfiguration.getOlmSourceName())
                .withSourceNamespace(olmConfiguration.getOlmSourceNamespace())
                .withChannel(olmConfiguration.getChannelName())
                .withInstallPlanApproval(olmConfiguration.getOlmInstallationStrategy().toString())
                .editOrNewConfig()
                    .withEnv(olmConfiguration.getAllEnvVariablesForOlm())
                .endConfig()
            .endSpec()
            .build();

        ResourceManager.getInstance().createResource(olmConfiguration.getExtensionContext(), subscription);
    }

    /**
     * Approves non-approved InstallPlan.
     * Used for manual installation type.
     */
    private void approveNotApprovedInstallPlan() {
        String notApprovedIPName = kubeClient().getInstallPlanNameUsingCsvPrefix(olmConfiguration.getNamespaceName(), olmConfiguration.getCsvName());
        kubeClient().approveInstallPlan(olmConfiguration.getNamespaceName(), notApprovedIPName);
    }

    /**
     * Upgrade cluster operator by obtaining new install plan, which was not used and also approves the installation
     */
    public void upgradeClusterOperator() {
        if (kubeClient().listPodsByPrefixInName(ResourceManager.getCoDeploymentName()).size() == 0) {
            throw new RuntimeException("We can not perform upgrade! Cluster operator pod is not present.");
        }
        approveNotApprovedInstallPlan();
    }

    public void updateSubscription(OlmConfiguration olmConfiguration) {
        this.olmConfiguration = olmConfiguration;
        // add CSV resource to the end of the stack -> to be deleted after the subscription and operator group
        ResourceManager.STORED_RESOURCES.get(olmConfiguration.getExtensionContext().getDisplayName())
            .add(ResourceManager.STORED_RESOURCES.get(olmConfiguration.getExtensionContext().getDisplayName()).size(), new ResourceItem(this::deleteCSV));
        createAndModifySubscription();
    }

    private Map<String, JsonObject> parseExamplesFromCsv() {
        JsonArray examples = new JsonArray(kubeClient().getCsv(olmConfiguration.getNamespaceName(), olmConfiguration.getCsvName()).getMetadata().getAnnotations().get("alm-examples"));
        return examples.stream().map(o -> (JsonObject) o).collect(Collectors.toMap(object -> object.getString("kind"), object -> object));
    }

    public static Map<String, JsonObject> getExampleResources() {
        return exampleResources;
    }

    public OlmConfiguration getOlmConfiguration() {
        return olmConfiguration;
    }

    private void deleteCSV() {
        LOGGER.info("Deleting CSV {}/{}", olmConfiguration.getNamespaceName(), olmConfiguration.getCsvName());
        kubeClient().deleteCsv(olmConfiguration.getNamespaceName(), olmConfiguration.getCsvName());
    }
}
