/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2List;
import io.strimzi.test.CrdUtils;
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
public class KafkaMirrorMaker2CrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, KafkaMirrorMaker2, KafkaMirrorMaker2List> {
    protected static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMaker2CrdOperatorIT.class);

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, KafkaMirrorMaker2.class, KafkaMirrorMaker2List.class, KafkaMirrorMaker2.RESOURCE_KIND);
    }

    @Override
    protected String getCrd() {
        return CrdUtils.CRD_KAFKA_MIRROR_MAKER_2;
    }

    @Override
    protected String getCrdName() {
        return CrdUtils.CRD_KAFKA_MIRROR_MAKER_2_NAME;
    }

    @Override
    protected String getNamespace() {
        return "kafka-mirror-maker-2-crd-it-namespace";
    }

    @Override
    protected KafkaMirrorMaker2 getResource(String resourceName) {
        return new KafkaMirrorMaker2Builder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                    .withNewTarget()
                        .withAlias("target")
                        .withBootstrapServers("target:9092")
                        .withGroupId("my-group")
                        .withConfigStorageTopic("my-configs")
                        .withOffsetStorageTopic("my-offsets")
                        .withStatusStorageTopic("my-statuses")
                    .endTarget()
                    .addNewMirror()
                        .withNewSource()
                            .withAlias("source")
                            .withBootstrapServers("source:9092")
                        .endSource()
                        .withNewSourceConnector()
                        .endSourceConnector()
                    .endMirror()
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