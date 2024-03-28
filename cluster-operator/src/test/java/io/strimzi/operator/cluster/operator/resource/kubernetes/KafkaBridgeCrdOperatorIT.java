/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.common.InlineLogging;
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
public class KafkaBridgeCrdOperatorIT extends AbstractCustomResourceOperatorIT<KubernetesClient, KafkaBridge, KafkaBridgeList> {
    protected static final Logger LOGGER = LogManager.getLogger(KafkaBridgeCrdOperatorIT.class);

    @Override
    protected CrdOperator operator() {
        return new CrdOperator(vertx, client, KafkaBridge.class, KafkaBridgeList.class, KafkaBridge.RESOURCE_KIND);
    }

    @Override
    protected String getCrd() {
        return TestUtils.CRD_KAFKA_BRIDGE;
    }

    @Override
    protected String getCrdName() {
        return KafkaBridge.CRD_NAME;
    }

    @Override
    protected String getNamespace() {
        return "bridge-crd-it-namespace";
    }

    @Override
    protected KafkaBridge getResource(String resourceName) {
        return new KafkaBridgeBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(getNamespace())
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("localhost:9092")
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