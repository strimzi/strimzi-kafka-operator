/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;


import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectResources;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaConnectBuildUtilsTest {

    private static final String NAME = "my-connect";
    private static final String NAMESPACE = "my-connect-namespace";

    @Test
    void testMultiContainerPod() {
        String buildPodName = KafkaConnectResources.buildPodName(NAME);
        String messageImg = "my-connect-build@sha256:blablabla";

        Pod terminatedPod = new PodBuilder()
            .withNewMetadata()
                .withName(buildPodName)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .withNewStatus()
                .withContainerStatuses(
                    new ContainerStatusBuilder()
                    .withName(buildPodName)
                    .withNewState()
                        .withNewTerminated()
                            .withExitCode(0)
                            .withMessage(messageImg)
                        .endTerminated()
                    .endState()
                    .build(),
                    new ContainerStatusBuilder()
                        .withName("my-sidecar")
                        .withNewState()
                            .withNewTerminated()
                                .withExitCode(1)
                                .withMessage("Container failed with random error")
                            .endTerminated()
                        .endState()
                        .build())
                .endStatus()
            .build();

        ContainerStateTerminated state = KafkaConnectBuildUtils.getConnectBuildContainerStateTerminated(terminatedPod, buildPodName);

        assertNotNull(state);
        assertEquals(state.getMessage(), messageImg);

        assertTrue(KafkaConnectBuildUtils.buildPodSucceeded(terminatedPod, buildPodName));
    }
}
