/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.templates.specific;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.DeploymentTypes;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScraperTemplates {

    private ScraperTemplates() { }

    public static DeploymentBuilder scraperPod(String namespaceName, String podName) {
        Map<String, String> label = new HashMap<>();

        label.put(TestConstants.SCRAPER_LABEL_KEY, TestConstants.SCRAPER_LABEL_VALUE);
        label.put(TestConstants.DEPLOYMENT_TYPE, DeploymentTypes.Scraper.name());

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isBlank()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        return new DeploymentBuilder()
            .withNewMetadata()
                .withName(podName)
                .withLabels(label)
                .withNamespace(namespaceName)
            .endMetadata()
            .withNewSpec()
                .withNewSelector()
                    .addToMatchLabels("app", podName)
                    .addToMatchLabels(label)
                .endSelector()
                .withReplicas(1)
                .withNewTemplate()
                    .withNewMetadata()
                        .addToLabels("app", podName)
                        .addToLabels(label)
                    .endMetadata()
                    .withNewSpecLike(podSpecBuilder.build())
                        .withContainers(
                            new ContainerBuilder()
                                .withName(podName)
                                .withImage(Environment.SCRAPER_IMAGE)
                                .withCommand("sleep")
                                .withArgs("infinity")
                                .withImagePullPolicy(Environment.COMPONENTS_IMAGE_PULL_POLICY)
                                .withResources(new ResourceRequirementsBuilder()
                                    .addToRequests("memory", new Quantity("200M"))
                                    .build())
                                .build()
                        )
                        .endSpec()
                .endTemplate()
            .endSpec();
    }
}
