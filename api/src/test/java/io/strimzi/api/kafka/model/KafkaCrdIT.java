/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

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
    void testKafkaIsNotScaling() {
        assertThrows(KubeClusterException.class, () -> createScaleDelete(Kafka.class, "Kafka.yaml"));
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
            () -> createDelete(Kafka.class, "Kafka-with-missing-required-property.yaml"));

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.zookeeper", "spec.kafka");
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

        assertThat(exception.getMessage(),
                containsStringIgnoringCase("spec.maintenanceTimeWindows in body must be of type string: \"null\""));
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

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.kafka.tlsSidecar.logLevel in body should be one of [emerg alert crit err warning notice info debug]"),
                containsStringIgnoringCase("spec.kafka.tlsSidecar.logLevel: Unsupported value: \"invalid\": supported values: \"emerg\", \"alert\", \"crit\", \"err\", \"warning\", \"notice\", \"info\", \"debug\"")));
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

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.zookeeper.storage.type in body should be one of [ephemeral persistent-claim]"),
                containsStringIgnoringCase("spec.zookeeper.storage.type: Unsupported value: \"jbod\": supported values: \"ephemeral\", \"persistent-claim\"")));
    }

    @Test
    public void testKafkaWithInvalidStorage() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "Kafka-with-invalid-storage.yaml");
            });

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.kafka.storage.type in body should be one of [ephemeral persistent-claim jbod]"),
                containsStringIgnoringCase("spec.kafka.storage.type: Unsupported value: \"foobar\": supported values: \"ephemeral\", \"persistent-claim\", \"jbod\"")));
    }

    @Test
    public void testKafkaWithInvalidJmxAuthentication() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "Kafka-with-invalid-jmx-authentication.yaml");
            });

        assertThat(exception.getMessage(), anyOf(
                containsStringIgnoringCase("spec.kafka.jmxOptions.authentication.type in body should be one of [password]"),
                containsStringIgnoringCase("spec.kafka.jmxOptions.authentication.type: Unsupported value: \"not-right\": supported values: \"password\"")));
    }

    @Test
    void testJmxOptionsWithoutRequiredOutputDefinitionKeys() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "JmxTrans-output-definition-with-missing-required-property.yaml");
            });

        assertMissingRequiredPropertiesMessage(exception.getMessage(), "spec.jmxTrans.outputDefinitions.outputType", "spec.jmxTrans.outputDefinitions.name");
    }

    @Test
    void testJmxOptionsWithoutRequiredQueryKeys() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "JmxTrans-queries-with-missing-required-property.yaml");
            });

        assertMissingRequiredPropertiesMessage(exception.getMessage(),
                "spec.jmxTrans.kafkaQueries.targetMBean",
                "spec.jmxTrans.kafkaQueries.attributes",
                "spec.jmxTrans.kafkaQueries.outputs");
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
