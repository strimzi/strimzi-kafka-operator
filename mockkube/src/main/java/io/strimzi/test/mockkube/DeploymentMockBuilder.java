/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatusBuilder;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class DeploymentMockBuilder extends MockBuilder<Deployment, DeploymentList, DoneableDeployment, RollableScalableResource<Deployment,
                DoneableDeployment>> {
    private static final Logger LOGGER = LogManager.getLogger(DeploymentMockBuilder.class);

    private final MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods;
    private Map<String, List<String>> podsForDeployments = new HashMap<>();

    public DeploymentMockBuilder(Map<String, Deployment> depDb, MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods) {
        super(Deployment.class, DeploymentList.class, DoneableDeployment.class, castClass(RollableScalableResource.class), depDb);
        this.mockPods = mockPods;
    }

    @Override
    protected void mockCreate(String resourceName, RollableScalableResource<Deployment, DoneableDeployment> resource) {
        when(resource.create(any(Deployment.class))).thenAnswer(invocation -> {
            checkNotExists(resourceName);
            Deployment deployment = invocation.getArgument(0);
            LOGGER.debug("create {} {} -> {}", resourceType, resourceName, deployment);
            deployment.getMetadata().setGeneration(Long.valueOf(0));
            deployment.setStatus(new DeploymentStatusBuilder().withObservedGeneration(Long.valueOf(0)).build());
            db.put(resourceName, copyResource(deployment));
            for (int i = 0; i < deployment.getSpec().getReplicas(); i++) {
                String uuid = UUID.randomUUID().toString();
                String podName = deployment.getMetadata().getName() + "-" + uuid;
                LOGGER.debug("create Pod {} because it's in Deployment {}", podName, resourceName);
                Pod pod = new PodBuilder()
                        .withNewMetadataLike(deployment.getSpec().getTemplate().getMetadata())
                            .withUid(uuid)
                            .withNamespace(deployment.getMetadata().getNamespace())
                            .withName(podName)
                        .endMetadata()
                        .withNewSpecLike(deployment.getSpec().getTemplate().getSpec()).endSpec()
                        .build();
                mockPods.inNamespace(deployment.getMetadata().getNamespace()).withName(podName).create(pod);
                podsForDeployments.compute(deployment.getMetadata().getName(), (deploymentName, podsInDeployment) -> {
                    if (podsInDeployment == null) {
                        podsInDeployment = new ArrayList<>(2);
                    }
                    podsInDeployment.add(podName);
                    return podsInDeployment;
                });
            }
            return deployment;
        });
    }

    @Override
    protected void mockPatch(String resourceName, RollableScalableResource<Deployment, DoneableDeployment> resource) {
        when(resource.patch(any())).thenAnswer(invocation -> {
            Deployment deployment = invocation.getArgument(0);
            String deploymentName = deployment.getMetadata().getName();
            // Initialize the map with empty collection in cases where deployment was initialized with zero replicas
            podsForDeployments.putIfAbsent(deploymentName, new ArrayList<>());

            deployment.getMetadata().setGeneration(Long.valueOf(0));
            deployment.setStatus(new DeploymentStatusBuilder().withObservedGeneration(Long.valueOf(0)).build());
            LOGGER.debug("patched {} {} -> {}", resourceType, resourceName, deployment);
            db.put(resourceName, copyResource(deployment));

            // Handle case where patch reduces replicas
            int podsToDelete = podsForDeployments.get(deploymentName).size() - deployment.getSpec().getReplicas();
            if (podsToDelete > 0) {
                for (int i = 0; i < podsToDelete; i++) {
                    String podToDelete = podsForDeployments.get(deploymentName).remove(0);
                    mockPods.inNamespace(deployment.getMetadata().getNamespace()).withName(podToDelete).delete();
                }
            }

            List<String> newPodNames = new ArrayList<>();
            for (int i = 0; i < deployment.getSpec().getReplicas(); i++) {
                // create a "new" Pod
                String uuid = UUID.randomUUID().toString();
                String newPodName = deploymentName + "-" + uuid;

                Pod newPod = new PodBuilder()
                        .withNewMetadataLike(deployment.getSpec().getTemplate().getMetadata())
                            .withUid(uuid)
                            .withNamespace(deployment.getMetadata().getNamespace())
                            .withName(newPodName)
                        .endMetadata()
                        .withNewSpecLike(deployment.getSpec().getTemplate().getSpec()).endSpec()
                        .build();
                mockPods.inNamespace(deployment.getMetadata().getNamespace()).withName(newPodName).create(newPod);
                newPodNames.add(newPodName);

                // delete the first "old" Pod if there is one still remaining
                if (podsForDeployments.get(deploymentName).size() > 0) {
                    String podToDelete = podsForDeployments.get(deploymentName).remove(0);
                    mockPods.inNamespace(deployment.getMetadata().getNamespace()).withName(podToDelete).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
                }

            }
            podsForDeployments.get(deploymentName).addAll(newPodNames);

            return deployment;
        });
    }
}
