/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.StrimziPodSetList;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.test.TestUtils;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public class StrimziPodSetCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, StrimziPodSet, StrimziPodSetList> {
    protected static final Logger LOGGER = LogManager.getLogger(StrimziPodSetCrdOperatorIT.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, StrimziPodSet.class, StrimziPodSetList.class, StrimziPodSet.RESOURCE_KIND);
    }

    @Override
    protected String getCrd() {
        return TestUtils.CRD_STRIMZI_POD_SET;
    }

    @Override
    protected String getCrdName() {
        return StrimziPodSet.CRD_NAME;
    }

    @Override
    protected String getNamespace() {
        return "strimzipodset-crd-it-namespace";
    }

    @Override
    protected StrimziPodSet getResource(String resourceName) {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName("broker")
                    .withLabels(Map.of("role", "broker"))
                .endMetadata()
                .withNewSpec()
                    .withContainers(new ContainerBuilder().withName("broker").withImage("kafka:latest").build())
                .endSpec()
                .build();

        return new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(Map.of("role", "broker")).build())
                    .withPods(mapper.convertValue(pod, new TypeReference<Map<String, Object>>() { }))
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    @Override
    protected StrimziPodSet getResourceWithModifications(StrimziPodSet resourceInCluster) {
        Pod pod = new PodBuilder()
                .withNewMetadata()
                    .withName("broker2")
                    .withLabels(Map.of("role", "broker"))
                .endMetadata()
                .withNewSpec()
                    .withContainers(new ContainerBuilder().withName("broker").withImage("kafka:latest").build())
                .endSpec()
                .build();

        return new StrimziPodSetBuilder(resourceInCluster)
                .editSpec()
                    .addToPods(mapper.convertValue(pod, new TypeReference<Map<String, Object>>() { }))
                .endSpec()
                .build();

    }

    @Override
    protected StrimziPodSet getResourceWithNewReadyStatus(StrimziPodSet resourceInCluster) {
        return new StrimziPodSetBuilder(resourceInCluster)
                .withNewStatus()
                    .withConditions(READY_CONDITION)
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(VertxTestContext context, StrimziPodSet resource) {
        context.verify(() -> assertThat(resource.getStatus()
                .getConditions()
                .get(0), is(READY_CONDITION)));
    }
}