/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
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
public class KafkaCrdOperatorIT extends AbstractCustomResourceOperatorIT {
    protected static final Logger log = LogManager.getLogger(KafkaCrdOperatorIT.class);

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, Kafka.class, KafkaList.class, DoneableKafka.class);
    }

    @Override
    protected CustomResourceDefinition getCrd() {
        return Crds.kafka();
    }

    @Override
    protected String getNamespace() {
        return "kafka-crd-it-namespace";
    }

    protected Kafka getResource() {
        return new KafkaBuilder()
                .withApiVersion(Kafka.RESOURCE_GROUP + "/" + Kafka.V1BETA1)
                .withNewMetadata()
                .withName(RESOURCE_NAME)
                .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                .withNewKafka()
                .withReplicas(1)
                .withNewListeners()
                .withNewPlain()
                .endPlain()
                .endListeners()
                .withNewEphemeralStorage()
                .endEphemeralStorage()
                .endKafka()
                .withNewZookeeper()
                .withReplicas(1)
                .withNewEphemeralStorage()
                .endEphemeralStorage()
                .endZookeeper()
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    @Override
    protected CustomResource getResourceWithModifications(CustomResource resourceInCluster) {
        return new KafkaBuilder((Kafka) resourceInCluster)
                .editSpec()
                .editKafka()
                .addToConfig("xxx", "yyy")
                .endKafka()
                .endSpec()
                .build();

    }

    @Override
    protected CustomResource getResourceWithNewReadyStatus(CustomResource resourceInCluster) {
        return new KafkaBuilder((Kafka) resourceInCluster)
                .withNewStatus()
                .withConditions(READY_CONDITION)
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(VertxTestContext context, CustomResource modifiedCustomResource) {
        Kafka kafka = (Kafka) modifiedCustomResource;
        context.verify(() -> assertThat(kafka.getStatus()
                .getConditions()
                .get(0), is(READY_CONDITION)));
    }
}