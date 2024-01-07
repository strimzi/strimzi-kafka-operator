/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import io.strimzi.api.kafka.model.AbstractCrdIT;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

    @Test
    void testKafkaWithExtraProperty() {
        // oc tool does not fail with extra properties, it shows only a warning. So this test does not pass on OpenShift
        assumeKube();

        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("Kafka-with-extra-property.yaml"));

        assertThat(exception.getMessage(), anyOf(
                containsString("unknown field \"thisPropertyIsNotInTheSchema\""),
                containsString("unknown field \"spec.thisPropertyIsNotInTheSchema\"")
        ));
    }

    @Test
    void testKafkaWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> createDeleteCustomResource("Kafka-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "kafka");
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
                anyOf(
                        containsStringIgnoringCase("invalid: spec.maintenanceTimeWindows: Invalid value: \"null\": spec.maintenanceTimeWindows in body must be of type string: \"null\""),
                        containsStringIgnoringCase("invalid: spec.maintenanceTimeWindows[0]: Invalid value: \"null\": spec.maintenanceTimeWindows[0] in body must be of type string: \"null\""),
                        containsStringIgnoringCase("unknown object type \"nil\" in Kafka.spec.maintenanceTimeWindows[0]")
                ));
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
                containsStringIgnoringCase("unknown field \"volumes\" in io.strimzi.kafka.v1beta2.Kafka.spec.zookeeper.storage"),
                containsStringIgnoringCase("unknown field \"spec.zookeeper.storage.volumes\"")));
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
    public void testKafkaWithInvalidZookeeperJmxAuthentication() {
        Throwable exception = assertThrows(
            KubeClusterException.class,
            () -> {
                createDeleteCustomResource("Kafka-with-invalid-zookeeper-jmx-authentication.yaml");
            });

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.zookeeper.jmxOptions.authentication.type in body should be one of [password]"),
                containsStringIgnoringCase("spec.zookeeper.jmxOptions.authentication.type: Unsupported value: \"not-right\": supported values: \"password\"")));
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA);
        cluster.waitForCustomResourceDefinition("kafkas.kafka.strimzi.io");
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA);
        cluster.deleteNamespaces();
    }
}
