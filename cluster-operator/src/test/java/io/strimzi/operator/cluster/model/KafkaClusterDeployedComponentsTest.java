/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class KafkaClusterDeployedComponentsTest {
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName("my-cluster")
                .withNamespace("my-namespace")
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
                .endKafka()
            .endSpec()
            .build();

    @Test
    public void testNoComponents() {
        KafkaClusterDeployedComponents components = KafkaClusterDeployedComponents.fromCrd(KAFKA);

        assertThat(components, is(KafkaClusterDeployedComponents.NONE));
        assertThat(components.topicOperator(), is(false));
        assertThat(components.userOperator(), is(false));
        assertThat(components.kafkaExporter(), is(false));
        assertThat(components.cruiseControl(), is(false));
        assertThat(components.entityOperator(), is(false));
    }

    @Test
    public void testAllComponents() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                    .withNewExtraMetrics()
                    .endExtraMetrics()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();

        KafkaClusterDeployedComponents components = KafkaClusterDeployedComponents.fromCrd(kafka);

        assertThat(components, is(KafkaClusterDeployedComponents.ALL));
        assertThat(components.topicOperator(), is(true));
        assertThat(components.userOperator(), is(true));
        assertThat(components.kafkaExporter(), is(true));
        assertThat(components.extraMetrics(), is(true));
        assertThat(components.cruiseControl(), is(true));
        assertThat(components.entityOperator(), is(true));
    }

    @Test
    public void testEntityOperatorWithoutTopicOrUserOperator() {
        // Entity Operator section without the Topic or User Operator does not deploy anything
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        KafkaClusterDeployedComponents components = KafkaClusterDeployedComponents.fromCrd(kafka);

        assertThat(components, is(KafkaClusterDeployedComponents.NONE));
        assertThat(components.entityOperator(), is(false));
    }

    @Test
    public void testTopicOperatorOnly() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewTopicOperator()
                        .endTopicOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        KafkaClusterDeployedComponents components = KafkaClusterDeployedComponents.fromCrd(kafka);

        assertThat(components, is(new KafkaClusterDeployedComponents(true, false, false, false, false)));
        assertThat(components.entityOperator(), is(true));
    }

    @Test
    public void testUserOperatorOnly() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewEntityOperator()
                        .withNewUserOperator()
                        .endUserOperator()
                    .endEntityOperator()
                .endSpec()
                .build();

        KafkaClusterDeployedComponents components = KafkaClusterDeployedComponents.fromCrd(kafka);

        assertThat(components, is(new KafkaClusterDeployedComponents(false, true, false, false, false)));
        assertThat(components.entityOperator(), is(true));
    }

    @Test
    public void testKafkaExporterOnly() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewKafkaExporter()
                    .endKafkaExporter()
                .endSpec()
                .build();

        KafkaClusterDeployedComponents components = KafkaClusterDeployedComponents.fromCrd(kafka);

        assertThat(components, is(new KafkaClusterDeployedComponents(false, false, true, false, false)));
        assertThat(components.entityOperator(), is(false));
    }

    @Test
    public void testExtraMetricsOnly() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewExtraMetrics()
                    .endExtraMetrics()
                .endSpec()
                .build();

        KafkaClusterDeployedComponents components = KafkaClusterDeployedComponents.fromCrd(kafka);

        assertThat(components, is(new KafkaClusterDeployedComponents(false, false, false, true, false)));
        assertThat(components.entityOperator(), is(false));
    }

    @Test
    public void testCruiseControlOnly() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .withNewCruiseControl()
                    .endCruiseControl()
                .endSpec()
                .build();

        KafkaClusterDeployedComponents components = KafkaClusterDeployedComponents.fromCrd(kafka);

        assertThat(components, is(new KafkaClusterDeployedComponents(false, false, false, false, true)));
        assertThat(components.entityOperator(), is(false));
    }
}
