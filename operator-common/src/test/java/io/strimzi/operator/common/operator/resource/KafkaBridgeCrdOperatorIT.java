/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaBridgeList;
import io.strimzi.api.kafka.model.DoneableKafkaBridge;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.KafkaBridge;
import io.strimzi.api.kafka.model.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.status.ConditionBuilder;

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
public class KafkaBridgeCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, KafkaBridge, KafkaBridgeList, DoneableKafkaBridge> {
    protected static final Logger log = LogManager.getLogger(KafkaBridgeCrdOperatorIT.class);

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, KafkaBridge.class, KafkaBridgeList.class, DoneableKafkaBridge.class, Crds.kafkaBridge());
    }

    @Override
    protected CustomResourceDefinition getCrd() {
        return Crds.kafkaBridge();
    }

    @Override
    protected String getNamespace() {
        return "bridge-crd-it-namespace";
    }

    @Override
    protected KafkaBridge getResource(String resourceName) {
        return new KafkaBridgeBuilder()
                .withApiVersion(KafkaBridge.RESOURCE_GROUP + "/" + KafkaBridge.V1ALPHA1)
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
    protected KafkaBridge getResourceWithModifications(KafkaBridge resourceInCluster) {
        return new KafkaBridgeBuilder(resourceInCluster)
                .editSpec()
                .withLogging(new InlineLogging())
                .endSpec()
                .build();
    }

    @Override
    protected KafkaBridge getResourceWithNewReadyStatus(KafkaBridge resourceInCluster) {
        return new KafkaBridgeBuilder(resourceInCluster)
                .withNewStatus()
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(VertxTestContext context, KafkaBridge resource) {
        context.verify(() -> assertThat(resource.getStatus()
                .getConditions()
                .get(0), is(READY_CONDITION)));
    }
}