/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator.specific;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.strimzi.systemtest.TestConstants;
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

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public void create() {
        ResourceManager.STORED_RESOURCES.computeIfAbsent(olmConfiguration.getExtensionContext().getDisplayName(), k -> new Stack<>());

        createOperatorGroup();

        ResourceManager.STORED_RESOURCES.get(olmConfiguration.getExtensionContext().getDisplayName()).push(new ResourceItem(this::deleteCSV));

        createAndModifySubscription();

        // Manual installation needs to be approved with a patch
        if (olmConfiguration.getOlmInstallationStrategy() == OlmInstallationStrategy.Manual) {
            OlmUtils.waitUntilNonUsedInstallPlanWithSpecificCsvIsPresentAndApprove(olmConfiguration.getNamespaceName(), olmConfiguration.getCsvName());
        }

        // Make sure that operator will be created
        TestUtils.waitFor("Cluster Operator deployment creation", TestConstants.GLOBAL_POLL_INTERVAL, CR_CREATION_TIMEOUT,
            () -> kubeClient(olmConfiguration.getNamespaceName()).getDeploymentNameByPrefix(olmConfiguration.getOlmOperatorDeploymentName()) != null);

        deploymentName = kubeClient(olmConfiguration.getNamespaceName()).getDeploymentNameByPrefix(olmConfiguration.getOlmOperatorDeploymentName());
        ResourceManager.setCoDeploymentName(deploymentName);

        // Wait for operator creation
        DeploymentUtils.waitForDeploymentAndPodsReady(olmConfiguration.getNamespaceName(), deploymentName, 1);

        exampleResources = parseExamplesFromCsv();
    }

    @Override
    public void delete() {
        // Deletion is done by ResourceManager using StoredResource
    }

    /**
     * Creates OperatorGroup in specific namespace
     */
    private void createOperatorGroup() {
        OperatorGroupBuilder operatorGroup = new OperatorGroupBuilder()
            .editOrNewMetadata()
                .withName("strimzi-group")
                .withNamespace(olmConfiguration.getNamespaceName())
                .withLabels(Collections.singletonMap("app", "strimzi"))
            .endMetadata();

        // single or multiple specific namespaces to watch
        if (!olmConfiguration.getNamespaceToWatch().equals(TestConstants.WATCH_ALL_NAMESPACES)) {
            operatorGroup
                .editOrNewSpec()
                    .withTargetNamespaces(olmConfiguration.getNamespaceToWatch())
                .endSpec();
        }

        ResourceManager.getInstance().createResourceWithWait(operatorGroup.build());
    }

    /**
     * Creates Subscription with spec from OlmConfiguration
     */

    private void createAndModifySubscription() {
        Subscription subscription = prepareSubscription(olmConfiguration);

        ResourceManager.getInstance().createResourceWithWait(subscription);
    }
    public void updateSubscription(OlmConfiguration olmConfiguration) {
        this.olmConfiguration = olmConfiguration;
        // add CSV resource to the end of the stack -> to be deleted after the subscription and operator group
        ResourceManager.STORED_RESOURCES
            .get(olmConfiguration.getExtensionContext().getDisplayName())
            .add(ResourceManager.STORED_RESOURCES
                    .get(olmConfiguration.getExtensionContext().getDisplayName()).size(),
                new ResourceItem(this::deleteCSV));
        Subscription subscription = prepareSubscription(this.olmConfiguration);
        // This is just a workaround until we implement update in ResourceManager
        ResourceManager.kubeClient().getClient().adapt(OpenShiftClient.class).operatorHub().subscriptions()
                .inNamespace(subscription.getMetadata().getNamespace())
                .resource(subscription)
                .patch();
    }

    public Subscription prepareSubscription(OlmConfiguration olmConfiguration) {
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
                .withStartingCSV(olmConfiguration.getCsvName())
                .withInstallPlanApproval(olmConfiguration.getOlmInstallationStrategy().toString())
                .editOrNewConfig()
                    .withEnv(olmConfiguration.getAllEnvVariablesForOlm())
                .endConfig()
            .endSpec()
            .build();

        // Change default values for Cluster Operator memory when RESOURCE_ALLOCATION_STRATEGY is not set to NOT_SHARED
        if (KubeClusterResource.getInstance().fipsEnabled()) {
            ResourceRequirements resourceRequirements = new ResourceRequirementsBuilder()
                    .withRequests(Map.of("memory", new Quantity(TestConstants.CO_REQUESTS_MEMORY), "cpu", new Quantity(
                        TestConstants.CO_REQUESTS_CPU)))
                    .withLimits(Map.of("memory", new Quantity(TestConstants.CO_LIMITS_MEMORY), "cpu", new Quantity(
                        TestConstants.CO_LIMITS_CPU)))
                    .build();

            subscription.getSpec().getConfig().setResources(resourceRequirements);
        }

        return subscription;
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
