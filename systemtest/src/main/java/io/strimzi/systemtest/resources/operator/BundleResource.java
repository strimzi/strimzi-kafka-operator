/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.ResourceType;
import io.strimzi.systemtest.resources.kubernetes.DeploymentResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class BundleResource implements ResourceType<Deployment> {
    private static final Logger LOGGER = LogManager.getLogger(BundleResource.class);

    public static final String PATH_TO_CO_CONFIG = TestUtils.USER_PATH + "/../install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml";

    @Override
    public String getKind() {
        return "Deployment";
    }
    @Override
    public Deployment get(String namespace, String name) {
        return ResourceManager.kubeClient().getDeployment(ResourceManager.kubeClient().getDeploymentNameByPrefix(name));
    }
    @Override
    public void create(Deployment resource) {
        ResourceManager.kubeClient().createOrReplaceDeployment(resource);
    }
    @Override
    public void delete(Deployment resource) throws Exception {
        ResourceManager.kubeClient().namespace(resource.getMetadata().getNamespace()).deleteDeployment(resource.getMetadata().getName());
    }
    @Override
    public boolean isReady(Deployment resource) {
        LOGGER.info("==========================================================");
        LOGGER.info(resource);

        Deployment[] deployment = new Deployment[1];
        deployment[0] = resource;

        TestUtils.waitFor(" for resource: " + resource + " to be present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            deployment[0] = ResourceManager.kubeClient().getDeployment(ResourceManager.kubeClient().getDeploymentNameByPrefix(Constants.STRIMZI_DEPLOYMENT_NAME));
            LOGGER.info("Resource is present: {}", deployment[0] != null);
            return deployment[0] != null;
        });

        return DeploymentUtils.waitForDeploymentReady(deployment[0].getMetadata().getName());
    }
    @Override
    public void refreshResource(Deployment existing, Deployment newResource) {
        Deployment[] deployment = new Deployment[1];
        deployment[0] = existing;

        TestUtils.waitFor(" for resource: " + existing + " to be present", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT, () -> {
            deployment[0] = ResourceManager.kubeClient().getDeployment(ResourceManager.kubeClient().getDeploymentNameByPrefix(Constants.STRIMZI_DEPLOYMENT_NAME));
            LOGGER.info("Resource is present: {}", deployment[0] != null);
            return deployment[0] != null;
        });

        deployment[0].setMetadata(newResource.getMetadata());
        deployment[0].setSpec(newResource.getSpec());
        deployment[0].setStatus(newResource.getStatus());
    }
    public static DeploymentBuilder clusterOperator(String namespace, long operationTimeout) {
        return defaultClusterOperator(Constants.STRIMZI_DEPLOYMENT_NAME, namespace, operationTimeout, Constants.RECONCILIATION_INTERVAL);
    }

    public static DeploymentBuilder clusterOperator(String namespace, long operationTimeout, long reconciliationInterval) {
        return defaultClusterOperator(Constants.STRIMZI_DEPLOYMENT_NAME, namespace, operationTimeout, reconciliationInterval);
    }

    public static DeploymentBuilder clusterOperator(String name, String namespace, long operationTimeout, long reconciliationInterval) {
        return defaultClusterOperator(name, namespace, operationTimeout, reconciliationInterval);
    }

    public static DeploymentBuilder clusterOperator(String namespace) {
        return defaultClusterOperator(Constants.STRIMZI_DEPLOYMENT_NAME, namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    public static DeploymentBuilder defaultClusterOperator(String namespace) {
        return defaultClusterOperator(Constants.STRIMZI_DEPLOYMENT_NAME, namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    private static DeploymentBuilder defaultClusterOperator(String name, String namespace, long operationTimeout, long reconciliationInterval) {

        Deployment clusterOperator = DeploymentResource.getDeploymentFromYaml(PATH_TO_CO_CONFIG);

        // Get env from config file
        List<EnvVar> envVars = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        // Get default CO image
        String coImage = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();

        // Update images
        for (EnvVar envVar : envVars) {
            switch (envVar.getName()) {
                case "STRIMZI_NAMESPACE":
                    envVar.setValue(namespace);
                    envVar.setValueFrom(null);
                    break;
                case "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS":
                    envVar.setValue(Long.toString(reconciliationInterval));
                    break;
                case "STRIMZI_OPERATION_TIMEOUT_MS":
                    envVar.setValue(Long.toString(operationTimeout));
                    break;
                default:
                    if (envVar.getName().contains("KAFKA_BRIDGE_IMAGE")) {
                        envVar.setValue(Environment.useLatestReleasedBridge() ? envVar.getValue() : Environment.BRIDGE_IMAGE);
                    } else if (envVar.getName().contains("STRIMZI_DEFAULT")) {
                        envVar.setValue(StUtils.changeOrgAndTag(envVar.getValue()));
                    } else if (envVar.getName().contains("IMAGES")) {
                        envVar.setValue(StUtils.changeOrgAndTagInImageMap(envVar.getValue()));
                    }
            }
        }

        envVars.add(new EnvVar("STRIMZI_IMAGE_PULL_POLICY", Environment.COMPONENTS_IMAGE_PULL_POLICY, null));
        envVars.add(new EnvVar("STRIMZI_LOG_LEVEL", Environment.STRIMZI_LOG_LEVEL, null));
        envVars.add(new EnvVar("STRIMZI_RBAC_SCOPE", Environment.STRIMZI_RBAC_SCOPE, null));
        // Apply updated env variables
        clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(envVars);

        return new DeploymentBuilder(clusterOperator)
            .editMetadata()
                .withName(name)
                .addToLabels("deployment-type", DeploymentTypes.BundleClusterOperator.name())
            .endMetadata()
            .editSpec()
                .withNewSelector()
                    .addToMatchLabels("name", Constants.STRIMZI_DEPLOYMENT_NAME)
                .endSelector()
                .editTemplate()
                    .editSpec()
                        .editFirstContainer()
                            .withImage(StUtils.changeOrgAndTag(coImage))
                            .withImagePullPolicy(Environment.OPERATOR_IMAGE_PULL_POLICY)
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec();
    }
}
