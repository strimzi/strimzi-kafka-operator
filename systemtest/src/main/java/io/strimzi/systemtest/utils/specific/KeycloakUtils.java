/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.strimzi.systemtest.resources.KubernetesResource;
import io.strimzi.systemtest.resources.ResourceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class KeycloakUtils {

    private static final Logger LOGGER = LogManager.getLogger(KeycloakUtils.class);

    private KeycloakUtils() {}

    public static DoneableDeployment deployKeycloak() {
        String keycloakName = "keycloak";

        Map<String, String> keycloakLabels = new HashMap<>();
        keycloakLabels.put("app", keycloakName);

        return KubernetesResource.deployNewDeployment(new DeploymentBuilder()
            .withNewMetadata()
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .withLabels(keycloakLabels)
                .withName(keycloakName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .withMatchLabels(keycloakLabels)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .withLabels(keycloakLabels)
                    .endMetadata()
                    .withNewSpec()
                        .withContainers()
                        .addNewContainer()
                            .withName(keycloakName + "pod")
                            .withImage("jboss/keycloak:8.0.1")
                            .withPorts(
                                new ContainerPortBuilder()
                                    .withName("http")
                                    .withContainerPort(8080)
                                    .build(),
                                new ContainerPortBuilder()
                                    .withName("https")
                                    .withContainerPort(8443)
                                    .build()
                            )
                            .addNewEnv()
                                .withName("KEYCLOAK_USER")
                                .withValue("admin")
                            .endEnv()
                            .addNewEnv()
                                .withName("KEYCLOAK_PASSWORD")
                                .withValue("admin")
                            .endEnv()
                            // for enabling importing authorization script
                            .withArgs("-Dkeycloak.profile.feature.upload_scripts=enabled")
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build());
    }
}
