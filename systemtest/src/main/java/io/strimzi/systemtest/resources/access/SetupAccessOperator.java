/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.access;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.draincleaner.SetupDrainCleaner;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.List;

public class SetupAccessOperator {

    public static final String PATH_TO_KAO_CONFIG = TestUtils.USER_PATH + "/../packaging/install/access-operator/";
    private static final Logger LOGGER = LogManager.getLogger(SetupDrainCleaner.class);

    /**
     * Method for installing Kafka Access Operator into the specified Namespace.
     *
     * @param namespaceName     Name of the Namespace where the KAO should be installed
     */
    public static void install(String namespaceName) {
        LOGGER.info("Applying files from path: {}", PATH_TO_KAO_CONFIG);

        List<File> accessOperatorFiles = Arrays.stream(new File(PATH_TO_KAO_CONFIG).listFiles()).sorted()
            .filter(File::isFile)
            .toList();

        accessOperatorFiles.forEach(file -> {
            if (!file.getName().contains("Namespace")) {
                final String resourceType = file.getName().split("-")[1].split(".yaml")[0];

                switch (resourceType) {
                    case TestConstants.SERVICE_ACCOUNT:
                        ServiceAccount serviceAccount = ReadWriteUtils.readObjectFromYamlFilepath(file, ServiceAccount.class);
                        ResourceManager.getInstance().createResourceWithWait(new ServiceAccountBuilder(serviceAccount)
                            .editMetadata()
                                .withNamespace(namespaceName)
                            .endMetadata()
                            .build());
                        break;
                    case TestConstants.CLUSTER_ROLE:
                        ClusterRole clusterRole = ReadWriteUtils.readObjectFromYamlFilepath(file, ClusterRole.class);
                        ResourceManager.getInstance().createResourceWithWait(clusterRole);
                        break;
                    case TestConstants.CLUSTER_ROLE_BINDING:
                        ClusterRoleBinding clusterRoleBinding = ReadWriteUtils.readObjectFromYamlFilepath(file, ClusterRoleBinding.class);
                        ResourceManager.getInstance().createResourceWithWait(new ClusterRoleBindingBuilder(clusterRoleBinding)
                            .editFirstSubject()
                                .withNamespace(namespaceName)
                            .endSubject()
                            .build()
                        );
                        break;
                    case TestConstants.DEPLOYMENT:
                        Deployment deployment = ReadWriteUtils.readObjectFromYamlFilepath(file, Deployment.class);
                        ResourceManager.getInstance().createResourceWithWait(new DeploymentBuilder(deployment)
                            .editMetadata()
                                .withNamespace(namespaceName)
                                .addToLabels(TestConstants.DEPLOYMENT_TYPE, DeploymentTypes.AccessOperator.name())
                            .endMetadata()
                            .build()
                        );
                        break;
                    case TestConstants.CUSTOM_RESOURCE_DEFINITION_SHORT:
                        CustomResourceDefinition customResourceDefinition = ReadWriteUtils.readObjectFromYamlFilepath(file, CustomResourceDefinition.class);
                        ResourceManager.getInstance().createResourceWithWait(customResourceDefinition);
                        break;
                    default:
                        LOGGER.error("Unknown installation resource type: {}", resourceType);
                        throw new RuntimeException("Unknown installation resource type:" + resourceType);
                }
            }
        });
    }
}
