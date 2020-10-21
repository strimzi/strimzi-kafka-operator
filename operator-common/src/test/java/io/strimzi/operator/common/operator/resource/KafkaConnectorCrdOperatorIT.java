/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaConnectorList;
import io.strimzi.api.kafka.model.DoneableKafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnector;
import io.strimzi.api.kafka.model.KafkaConnectorBuilder;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The main purpose of the Integration Tests for the operators is to test them against a real Kubernetes cluster.
 * Real Kubernetes cluster has often some quirks such as some fields being immutable, some fields in the spec section
 * being created by the Kubernetes API etc. These things are hard to test with mocks. These IT tests make it easy to
 * test them against real clusters.
 */
@ExtendWith(VertxExtension.class)
public class KafkaConnectorCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, KafkaConnector, KafkaConnectorList, DoneableKafkaConnector> {
    protected static final Logger log = LogManager.getLogger(KafkaConnectorCrdOperatorIT.class);

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, KafkaConnector.class, KafkaConnectorList.class, DoneableKafkaConnector.class, Crds.kafkaConnector());
    }

    @Override
    protected CustomResourceDefinition getCrd() {
        return Crds.kafkaConnector();
    }

    @Override
    protected String getNamespace() {
        return "kafka-connector-crd-it-namespace";
    }

    @Override
    protected KafkaConnector getResource(String resourceName) {
        return new KafkaConnectorBuilder()
                .withApiVersion(KafkaConnector.RESOURCE_GROUP + "/" + KafkaConnector.V1ALPHA1)
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    @Override
    protected KafkaConnector getResourceWithModifications(KafkaConnector resourceInCluster) {
        return new KafkaConnectorBuilder(resourceInCluster)
                .editMetadata()
                    .addToLabels("newLabelKey", "newLabelValue")
                .endMetadata()
                .build();
    }

    @Override
    protected KafkaConnector getResourceWithNewReadyStatus(KafkaConnector resourceInCluster) {
        return new KafkaConnectorBuilder(resourceInCluster)
                .withNewStatus()
                    .withConditions(READY_CONDITION)
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(VertxTestContext context, KafkaConnector resource) {
        context.verify(() -> assertThat(resource.getStatus()
                .getConditions()
                .get(0), is(READY_CONDITION)));
    }
}