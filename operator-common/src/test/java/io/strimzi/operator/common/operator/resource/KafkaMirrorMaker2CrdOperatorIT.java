/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.client.KubernetesClient;
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
public class KafkaMirrorMaker2CrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List, DoneableKafkaMirrorMaker2> {
    protected static final Logger log = LogManager.getLogger(KafkaMirrorMaker2CrdOperatorIT.class);

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, DoneableKafkaMirrorMaker2.class, Crds.kafkaMirrorMaker2());
    }

    @Override
    protected CustomResourceDefinition getCrd() {
        return Crds.kafkaMirrorMaker2();
    }

    @Override
    protected String getNamespace() {
        return "kafka-mirror-maker-2-crd-it-namespace";
    }

    @Override
    protected KafkaMirrorMaker2 getResource(String resourceName) {
        return new KafkaMirrorMaker2Builder()
                .withApiVersion(KafkaMirrorMaker2.RESOURCE_GROUP + "/" + KafkaMirrorMaker2.V1ALPHA1)
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
    protected KafkaMirrorMaker2 getResourceWithModifications(KafkaMirrorMaker2 resourceInCluster) {
        return new KafkaMirrorMaker2Builder(resourceInCluster)
                .editSpec()
                    .withNewLivenessProbe()
                        .withInitialDelaySeconds(14)
                    .endLivenessProbe()
                .endSpec()
                .build();
    }

    @Override
    protected KafkaMirrorMaker2 getResourceWithNewReadyStatus(KafkaMirrorMaker2 resourceInCluster) {
        return new KafkaMirrorMaker2Builder(resourceInCluster)
                .withNewStatus()
                    .withConditions(READY_CONDITION)
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(VertxTestContext context, KafkaMirrorMaker2 resource) {
        context.verify(() -> assertThat(resource.getStatus()
                .getConditions()
                .get(0), is(READY_CONDITION)));
    }
}