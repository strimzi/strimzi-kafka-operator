/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The purpose of this test is to confirm that we can create a
 * resource from the POJOs, serialize it and create the resource in K8S.
 * I.e. that such instance resources obtained from POJOs are valid according to the schema
 * validation done by K8S.
 */
public class JmxTransCrdIT extends AbstractCrdIT {
    public static final String NAMESPACE = "jmxtrans-it";

    @Test
    void testJmxOptionsWithoutRequiredOutputDefinitionKeys() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "JmxTrans-output-definition-with-missing-required-property.yaml");
            });

        assertThat(exception.getMessage(), CoreMatchers.containsStringIgnoringCase("spec.jmxTrans.outputDefinitions.outputType in body is required"));
        assertThat(exception.getMessage(), CoreMatchers.containsStringIgnoringCase("spec.jmxTrans.outputDefinitions.name in body is required"));
    }

    @Test
    void testJmxOptionsWithoutRequiredQueryKeys() {
        Throwable exception = assertThrows(
            KubeClusterException.InvalidResource.class,
            () -> {
                createDelete(Kafka.class, "JmxTrans-queries-with-missing-required-property.yaml");
            });

        assertThat(exception.getMessage(), CoreMatchers.containsStringIgnoringCase("spec.jmxTrans.kafkaQueries.attributes in body is required"));
        assertThat(exception.getMessage(), CoreMatchers.containsStringIgnoringCase("spec.jmxTrans.kafkaQueries.targetMBean in body is required"));
        assertThat(exception.getMessage(), CoreMatchers.containsStringIgnoringCase("spec.jmxTrans.kafkaQueries.outputs in body is required"));
    }

    @BeforeAll
    void setupEnvironment() {
        cluster.createNamespace(NAMESPACE);
        cluster.createCustomResources(TestUtils.CRD_KAFKA);
    }

    @AfterAll
    void teardownEnvironment() {
        cluster.deleteCustomResources(TestUtils.CRD_KAFKA);
        cluster.deleteNamespaces();
    }

}
