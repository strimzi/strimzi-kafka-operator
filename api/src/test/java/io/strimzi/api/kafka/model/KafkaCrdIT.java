/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class KafkaCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkacrd-it";

    @Test
    void testKafkaIsNotScaling() {
        assertThrows(KubeClusterException.class, () -> createScaleDelete(Kafka.class, "Kafka.yaml"));
    }

    @Test
    void testKafkaMinimal() {
        createDeleteCustomResource("Kafka-minimal.yaml");
    }

    @Disabled("See https://github.com/strimzi/strimzi-kafka-operator/issues/4606")
    @Test
    void testCreateKafkaWithExtraProperty() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("Kafka-with-extra-property.yaml"));

        assertThat(exception.getMessage(), containsString("unknown field \"thisPropertyIsNotInTheSchema\""));
    }

    @Test
    void testKafkaWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("Kafka-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "zookeeper", "kafka");
    }

    @Test
    public void testKafkaWithEntityOperator() {
        createDeleteCustomResource("Kafka-with-entity-operator.yaml");
    }

    @Test
    public void testKafkaWithMaintenance() {
        createDeleteCustomResource("Kafka-with-maintenance.yaml");
    }

    @Test
    public void testKafkaWithNullMaintenance() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> {
                createDeleteCustomResource("Kafka-with-null-maintenance.yaml");
            });

        assertThat(exception.getMessage(),
                containsStringIgnoringCase("invalid: spec.maintenanceTimeWindows: Invalid value: \"null\": spec.maintenanceTimeWindows in body must be of type string: \"null\""));
    }

    @Test
    public void testKafkaWithTemplate() {
        createDeleteCustomResource("Kafka-with-template.yaml");
    }

    @Test
    public void testKafkaWithJbodStorage() {
        createDeleteCustomResource("Kafka-with-jbod-storage.yaml");
    }

    @Test
    public void testKafkaWithJbodStorageOnZookeeper() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> {
                createDeleteCustomResource("Kafka-with-jbod-storage-on-zookeeper.yaml");
            });

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.zookeeper.storage.type in body should be one of [ephemeral persistent-claim]"),
                containsStringIgnoringCase("spec.zookeeper.storage.type: Unsupported value: \"jbod\": supported values: \"ephemeral\", \"persistent-claim\""),
                containsStringIgnoringCase("unknown field \"volumes\" in io.strimzi.kafka.v1beta2.Kafka.spec.zookeeper.storage")));
    }

    @Test
    public void testKafkaWithInvalidStorage() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> {
                createDeleteCustomResource("Kafka-with-invalid-storage.yaml");
            });

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.kafka.storage.type in body should be one of [ephemeral persistent-claim jbod]"),
                containsStringIgnoringCase("spec.kafka.storage.type: Unsupported value: \"foobar\": supported values: \"ephemeral\", \"persistent-claim\", \"jbod\"")));
    }

    @Test
    public void testKafkaWithInvalidJmxAuthentication() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> {
                createDeleteCustomResource("Kafka-with-invalid-jmx-authentication.yaml");
            });

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.kafka.jmxOptions.authentication.type in body should be one of [password]"),
                containsStringIgnoringCase("spec.kafka.jmxOptions.authentication.type: Unsupported value: \"not-right\": supported values: \"password\"")));
    }

    @Test
    void testJmxOptionsWithoutRequiredOutputDefinitionKeys() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> {
                createDeleteCustomResource("JmxTrans-output-definition-with-missing-required-property.yaml");
            });

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "outputType", "name");
    }

    @Test
    void testJmxOptionsWithoutRequiredQueryKeys() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> {
                createDeleteCustomResource("JmxTrans-queries-with-missing-required-property.yaml");
            });

        assertMissingRequiredPropertiesMessage(exception.getMessage(),
                "targetMBean",
                "attributes",
                "outputs");
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA);
        waitForCrd("crd", "kafkas.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA);
        cluster.deleteNamespaces();
    }
}
