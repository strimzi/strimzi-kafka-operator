/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaMirrorMaker2List;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Builder;
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
public class KafkaMirrorMaker2CrdOperatorIT extends AbstractCustomResourceOperatorIT {
    protected static final Logger log = LogManager.getLogger(KafkaMirrorMaker2CrdOperatorIT.class);

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, DoneableKafkaMirrorMaker2.class);

    }

    @Override
    protected CustomResourceDefinition getCrd() {
        return Crds.kafkaMirrorMaker2();
    }

    @Override
    protected String getNamespace() {
        return "kafka-mirror-maker-2-crd-it-namespace";
    }

    protected KafkaMirrorMaker2 getResource() {
        return new KafkaMirrorMaker2Builder()
                .withApiVersion(KafkaMirrorMaker2.RESOURCE_GROUP + "/" + KafkaMirrorMaker2.V1ALPHA1)
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    @Override
    protected CustomResource getResourceWithModifications(CustomResource resourceInCluster) {
        return new KafkaMirrorMaker2Builder((KafkaMirrorMaker2) resourceInCluster)
                .editSpec()
                .withNewLivenessProbe()
                .withInitialDelaySeconds(14)
                .endLivenessProbe()
                .endSpec()
                .build();
    }

    @Override
    protected CustomResource getResourceWithNewReadyStatus(CustomResource resourceInCluster) {
        return new KafkaMirrorMaker2Builder((KafkaMirrorMaker2) resourceInCluster)
                .withNewStatus()
                .withConditions(READY_CONDITION)
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(VertxTestContext context, CustomResource modifiedCustomResource) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = (KafkaMirrorMaker2) modifiedCustomResource;
        context.verify(() -> assertThat(kafkaMirrorMaker2.getStatus()
                .getConditions()
                .get(0), is(READY_CONDITION)));
    }
}