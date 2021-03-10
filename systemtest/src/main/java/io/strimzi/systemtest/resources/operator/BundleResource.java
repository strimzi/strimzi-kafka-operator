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
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;

import java.util.List;

public class BundleResource {
    public static final String PATH_TO_CO_CONFIG = TestUtils.USER_PATH + "/../packaging/install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml";

    public static DeploymentBuilder clusterOperator(String namespace, long operationTimeout) {
        return defaultClusterOperator(namespace, operationTimeout, Constants.RECONCILIATION_INTERVAL);
    }

    public static DeploymentBuilder clusterOperator(String namespace, long operationTimeout, long reconciliationInterval) {
        return defaultClusterOperator(namespace, operationTimeout, reconciliationInterval);
    }

    public static DeploymentBuilder clusterOperator(String namespace) {
        return defaultClusterOperator(namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    public static DeploymentBuilder defaultClusterOperator(String namespace) {
        return defaultClusterOperator(namespace, Constants.CO_OPERATION_TIMEOUT_DEFAULT, Constants.RECONCILIATION_INTERVAL);
    }

    private static DeploymentBuilder defaultClusterOperator(String namespace, long operationTimeout, long reconciliationInterval) {

        Deployment clusterOperator = KubernetesResource.getDeploymentFromYaml(PATH_TO_CO_CONFIG);

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

    public static Deployment createAndWaitForReadiness(Deployment co) {
        return KubernetesResource.deployNewDeployment(co);
    }
}
