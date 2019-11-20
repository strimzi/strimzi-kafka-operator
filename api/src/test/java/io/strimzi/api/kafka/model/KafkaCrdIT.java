/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
@DisabledIfEnvironmentVariable(named = "TRAVIS", matches = "true")
public class KafkaCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "kafkacrd-it";

    @Test
    void testKafkaV1alpha1() {
        assumeKube1_11Plus();
        createDelete(Kafka.class, "KafkaV1alpha1.yaml");
    }

    @Test
    void testKafkaV1Beta1() {
        createDelete(Kafka.class, "KafkaV1beta1.yaml");
    }

    @Test
    void testKafkaMinimal() {
        createDelete(Kafka.class, "Kafka-minimal.yaml");
    }

    @Test
    void testKafkaWithExtraProperty() {
        createDelete(Kafka.class, "Kafka-with-extra-property.yaml");
    }

    @Test
    void testKafkaWithMissingRequired() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "Kafka-with-missing-required-property.yaml");
            });

        String expectedMsg1a = "spec.zookeeper in body is required";
        String expectedMsg1b = "spec.kafka in body is required";

        String expectedMsg2a = "spec.kafka: Required value";
        String expectedMsg2b = "spec.zookeeper: Required value";

        assertThat(exception.getMessage(), anyOf(
                allOf(containsStringIgnoringCase(expectedMsg1a), containsStringIgnoringCase(expectedMsg1b)),
                allOf(containsStringIgnoringCase(expectedMsg2a), containsStringIgnoringCase(expectedMsg2b))));
    }

    @Test
    void testKafkaWithInvalidResourceMemory() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "Kafka-with-invalid-resource-memory.yaml");
            });

        assertThat(exception.getMessage(),
                containsStringIgnoringCase("spec.kafka.resources.limits.memory in body should match '[0-9]+([kKmMgGtTpPeE]i?)?$'"));
    }

    @Test
    public void testKafkaWithEntityOperator() {
        createDelete(Kafka.class, "Kafka-with-entity-operator.yaml");
    }

    @Test
    public void testKafkaWithMaintenance() {
        createDelete(Kafka.class, "Kafka-with-maintenance.yaml");
    }

    @Test
    public void testKafkaWithNullMaintenance() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "Kafka-with-null-maintenance.yaml");
            });

        String expectedMsg1 = "spec.maintenanceTimeWindows in body must be of type string: \"null\"";

        assertThat(exception.getMessage(), containsStringIgnoringCase(expectedMsg1));
    }

    @Test
    public void testKafkaWithTemplate() {
        createDelete(Kafka.class, "Kafka-with-template.yaml");
    }

    @Test
    public void testKafkaWithTlsSidecarWithCustomConfiguration() {
        createDelete(Kafka.class, "Kafka-with-tls-sidecar-with-custom-configuration.yaml");
    }

    @Test
    public void testKafkaWithTlsSidecarWithInvalidLogLevel() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "Kafka-with-tls-sidecar-invalid-loglevel.yaml");
            });

        String expectedMsg1 = "spec.kafka.tlsSidecar.logLevel in body should be one of [emerg alert crit err warning notice info debug]";
        String expectedMsg2 = "spec.kafka.tlsSidecar.logLevel: Unsupported value: \"invalid\": supported values: \"emerg\", \"alert\", \"crit\", \"err\", \"warning\", \"notice\", \"info\", \"debug\"";

        assertThat(exception.getMessage(), anyOf(containsStringIgnoringCase(expectedMsg1), containsStringIgnoringCase(expectedMsg2)));
    }

    @Test
    public void testKafkaWithJbodStorage() {
        createDelete(Kafka.class, "Kafka-with-jbod-storage.yaml");
    }

    @Test
    public void testKafkaWithJbodStorageOnZookeeper() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "Kafka-with-jbod-storage-on-zookeeper.yaml");
            });

        String expectedMsg1 = "spec.zookeeper.storage.type in body should be one of [ephemeral persistent-claim]";
        String expectedMsg2 = "spec.zookeeper.storage.type: Unsupported value: \"jbod\": supported values: \"ephemeral\", \"persistent-claim\"";

        assertThat(exception.getMessage(), anyOf(containsStringIgnoringCase(expectedMsg1), containsStringIgnoringCase(expectedMsg2)));
    }

    @Override
    protected void teardownEnvForOperator() {
        super.teardownEnvForOperator();
    }

    @Test
    public void testKafkaWithInvalidStorage() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "Kafka-with-invalid-storage.yaml");
            });

        String expectedMsg1 = "spec.kafka.storage.type in body should be one of [ephemeral persistent-claim jbod]";
        String expectedMsg2 = "spec.kafka.storage.type: Unsupported value: \"foobar\": supported values: \"ephemeral\", \"persistent-claim\", \"jbod\"";

        assertThat(exception.getMessage(), anyOf(containsStringIgnoringCase(expectedMsg1), containsStringIgnoringCase(expectedMsg2)));
    }

    @BeforeAll
    void setupEnvironment() {
        createNamespace(NAMESPACE);
        createCustomResources(TestUtils.CRD_KAFKA);
    }

    @AfterAll
    void teardownEnvironment() {
        deleteCustomResources();
        deleteNamespaces();
    }
}
