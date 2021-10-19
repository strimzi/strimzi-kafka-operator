/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.draincleaner;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.admissionregistration.v1.ValidatingWebhookConfiguration;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SetupDrainCleaner {

    public static final String PATH_TO_DC_CONFIG = TestUtils.USER_PATH + "/../packaging/install/drain-cleaner/kubernetes";
    private static final Logger LOGGER = LogManager.getLogger(SetupDrainCleaner.class);

    public void applyInstallFiles(ExtensionContext extensionContext) {
        List<File> drainCleanerFiles = Arrays.stream(new File(PATH_TO_DC_CONFIG).listFiles()).sorted()
            .filter(File::isFile)
            .collect(Collectors.toList());

        drainCleanerFiles.forEach(file -> {
            if (!file.getName().contains("README") && !file.getName().contains("Namespace")) {
                final String resourceType = file.getName().split("-")[1].split(".yaml")[0];

                switch (resourceType) {
                    case Constants.CLUSTER_ROLE:
                        ClusterRole clusterRole = TestUtils.configFromYaml(file, ClusterRole.class);
                        ResourceManager.getInstance().createResource(extensionContext, clusterRole);
                        break;
                    case Constants.SERVICE_ACCOUNT:
                        ServiceAccount serviceAccount = TestUtils.configFromYaml(file, ServiceAccount.class);
                        ResourceManager.getInstance().createResource(extensionContext, new ServiceAccountBuilder(serviceAccount)
                            .editMetadata()
                                .withNamespace(Constants.DRAIN_CLEANER_NAMESPACE)
                            .endMetadata()
                            .build());
                        break;
                    case Constants.CLUSTER_ROLE_BINDING:
                        ClusterRoleBinding clusterRoleBinding = TestUtils.configFromYaml(file, ClusterRoleBinding.class);
                        ResourceManager.getInstance().createResource(extensionContext, new ClusterRoleBindingBuilder(clusterRoleBinding).build());
                        break;
                    case Constants.SECRET:
                        Secret secret = TestUtils.configFromYaml(file, Secret.class);
                        ResourceManager.getInstance().createResource(extensionContext, secret);
                        break;
                    case Constants.SERVICE:
                        Service service = TestUtils.configFromYaml(file, Service.class);
                        ResourceManager.getInstance().createResource(extensionContext, service);
                        break;
                    case Constants.VALIDATION_WEBHOOK_CONFIG:
                        ValidatingWebhookConfiguration webhookConfiguration = TestUtils.configFromYaml(file, ValidatingWebhookConfiguration.class);
                        ResourceManager.getInstance().createResource(extensionContext, webhookConfiguration);
                        break;
                    default:
                        LOGGER.error("Unknown installation resource type: {}", resourceType);
                        throw new RuntimeException("Unknown installation resource type:" + resourceType);
                }
            }
        });
    }

    public void createDrainCleaner(ExtensionContext extensionContext) {
        applyInstallFiles(extensionContext);
        ResourceManager.getInstance().createResource(extensionContext, new DrainCleanerResource().buildDrainCleanerDeployment().build());
    }
}
