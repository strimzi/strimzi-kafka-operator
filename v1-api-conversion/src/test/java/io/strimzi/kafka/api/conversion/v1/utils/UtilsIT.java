/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.utils;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.kafka.api.conversion.v1.cli.ConversionTestUtils;
import io.strimzi.test.CrdUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class UtilsIT {
    private static KubernetesClient client;

    @BeforeAll
    static void setupEnvironment() {
        client = new KubernetesClientBuilder().withConfig(new ConfigBuilder().build()).build();
        ConversionTestUtils.createOrUpdateCrd(client, CrdUtils.CRD_KAFKA_NAME, ConversionTestUtils.CRD_V1_KAFKA);
    }

    @AfterAll
    static void teardownEnvironment() {
        CrdUtils.deleteCrd(client, CrdUtils.CRD_KAFKA_NAME);
        client.close();
    }

    @Test
    public void testMissingVersion() {
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> Utils.checkCrdsHaveApiVersions(client, List.of("kafkas.kafka.strimzi.io"), ApiVersion.V1BETA2, ApiVersion.V1));
        assertThat(ex.getMessage(), containsString("CRD kafkas.kafka.strimzi.io is missing at least one of the required API versions [v1beta2, v1]"));
    }

    @Test
    public void testMissingCrd() {
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> Utils.checkCrdsHaveApiVersions(client, List.of("kafkaconnects.kafka.strimzi.io"), ApiVersion.V1BETA2, ApiVersion.V1));
        assertThat(ex.getMessage(), containsString("CRD kafkaconnects.kafka.strimzi.io is missing"));
    }
}
