/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.common.InlineLogging;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerBuilder;
import io.strimzi.api.kafka.model.mirrormaker.KafkaMirrorMakerList;
import io.strimzi.test.TestUtils;
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
public class KafkaMirrorMakerCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, KafkaMirrorMaker, KafkaMirrorMakerList> {
    protected static final Logger LOGGER = LogManager.getLogger(KafkaMirrorMakerCrdOperatorIT.class);

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, KafkaMirrorMaker.class, KafkaMirrorMakerList.class, KafkaMirrorMaker.RESOURCE_KIND);
    }

    @Override
    protected String getCrd() {
        return TestUtils.CRD_KAFKA_MIRROR_MAKER;
    }

    @Override
    protected String getCrdName() {
        return KafkaMirrorMaker.CRD_NAME;
    }

    @Override
    protected String getNamespace() {
        return "kafka-mirror-maker-2-crd-it-namespace";
    }

    @Override
    protected KafkaMirrorMaker getResource(String resourceName) {
        return new KafkaMirrorMakerBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                    .withNewConsumer()
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("my-group")
                    .endConsumer()
                    .withNewProducer()
                        .withBootstrapServers("localhost:9092")
                    .endProducer()
                    .withInclude(".*")
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    @Override
    protected KafkaMirrorMaker getResourceWithModifications(KafkaMirrorMaker resourceInCluster) {
        return new KafkaMirrorMakerBuilder(resourceInCluster)
                .editSpec()
                    .withLogging(new InlineLogging())
                .endSpec()
                .build();
    }

    @Override
    protected KafkaMirrorMaker getResourceWithNewReadyStatus(KafkaMirrorMaker resourceInCluster) {
        return new KafkaMirrorMakerBuilder(resourceInCluster)
                .withNewStatus()
                    .withConditions(READY_CONDITION)
                .endStatus()
                .build();
    }

    @Override
    protected void assertReady(VertxTestContext context, KafkaMirrorMaker resource) {
        context.verify(() -> assertThat(resource.getStatus()
                .getConditions()
                .get(0), is(READY_CONDITION)));
    }
}
