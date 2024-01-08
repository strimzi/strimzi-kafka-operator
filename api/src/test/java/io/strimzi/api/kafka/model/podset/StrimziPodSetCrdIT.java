/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.podset;

import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class StrimziPodSetCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "strimzipodset-crd-it";

    @Test
    void testStrimziPodSetMinimal() {
        createDeleteCustomResource("StrimziPodSet.yaml");
    }

    @Test
    void testStrimziPodSettWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("StrimziPodSet-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "pods", "selector");
    }

    @Test
    void testZeroReplicas() {
        StrimziPodSet podSet = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-pod-set")
                .endMetadata()
                .withNewSpec()
                    .withPods(List.of())
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("label", "value")).build())
                .endSpec()
                .build();

        KubernetesClient client = new KubernetesClientBuilder().build();
        Crds.strimziPodSetOperation(client).inNamespace(NAMESPACE).resource(podSet).create();

        StrimziPodSet createdPodSet = Crds.strimziPodSetOperation(client).inNamespace(NAMESPACE).withName("my-pod-set").get();

        assertThat(createdPodSet.getSpec().getPods(), is(List.of()));

        Crds.strimziPodSetOperation(client).inNamespace(NAMESPACE).withName("my-pod-set").delete();
    }

    @BeforeAll
    void setupEnvironment() throws InterruptedException {
        cluster.createCustomResources(TestUtils.CRD_STRIMZI_POD_SET);
        cluster.waitForCustomResourceDefinition("strimzipodsets.core.strimzi.io");
        cluster.createNamespace(NAMESPACE);
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources();
        cluster.deleteNamespaces();
    }
}

