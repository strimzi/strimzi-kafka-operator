/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

public class SslConfigurationST extends SecurityST {

    private static final Logger LOGGER = LogManager.getLogger(SslConfigurationST.class);

    @Test
    void testKafkaAndKafkaConnectTlsVersion() {

        Map<String, Object> configWithNewestVersionOfTls = new HashMap<>();

        final String tlsVersion12 = "TLSv1.2";
        final String tlsVersion1 = "TLSv1";

        configWithNewestVersionOfTls.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsVersion12);
        configWithNewestVersionOfTls.put(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL);

        LOGGER.info("Deploying Kafka cluster with the support {} TLS",  tlsVersion12);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withConfig(configWithNewestVersionOfTls)
                .endKafka()
            .endSpec()
            .done();

        Map<String, Object> configsFromKafkaCustomResource = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getKafka().getConfig();

        LOGGER.info("Verifying that Kafka cluster has the excepted configuration:\n" +
                "" + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG + " -> {}\n" +
                "" + SslConfigs.SSL_PROTOCOL_CONFIG + " -> {}",
            configsFromKafkaCustomResource.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG),
            configsFromKafkaCustomResource.get(SslConfigs.SSL_PROTOCOL_CONFIG));

        assertThat(configsFromKafkaCustomResource.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG), is(tlsVersion12));
        assertThat(configsFromKafkaCustomResource.get(SslConfigs.SSL_PROTOCOL_CONFIG), is(SslConfigs.DEFAULT_SSL_PROTOCOL));

        Map<String, Object> configWithLowestVersionOfTls = new HashMap<>();

        configWithLowestVersionOfTls.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsVersion1);
        configWithLowestVersionOfTls.put(SslConfigs.SSL_PROTOCOL_CONFIG, tlsVersion1);

        KafkaClientsResource.deployKafkaClients(KAFKA_CLIENTS_NAME).done();

        KafkaConnectResource.kafkaConnectWithoutWait(KafkaConnectResource.defaultKafkaConnect(CLUSTER_NAME, CLUSTER_NAME, 1)
            .editSpec()
                .withConfig(configWithLowestVersionOfTls)
            .endSpec()
            .build());

        PodUtils.waitUntilPodIsPresent(KafkaConnectResources.deploymentName(CLUSTER_NAME)); // pod is not ready...

        LOGGER.info("Replacing Kafka connect config to the newest(TLSv1.2) one same as the Kafka broker has.");
        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kafkaConnect -> kafkaConnect.getSpec().setConfig(configWithNewestVersionOfTls));

        Map<String, Object> configsFromKafkaConnectCustomResource = KafkaConnectResource.kafkaConnectClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getConfig();

        LOGGER.info("Verifying that Kafka connect has the excepted configuration:\n" +
                "" + SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG + " -> {}\n" +
                "" + SslConfigs.SSL_PROTOCOL_CONFIG + " -> {}",
            configsFromKafkaConnectCustomResource.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG),
            configsFromKafkaConnectCustomResource.get(SslConfigs.SSL_PROTOCOL_CONFIG));

        assertThat(configsFromKafkaConnectCustomResource.get(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG), is(tlsVersion12));
        assertThat(configsFromKafkaConnectCustomResource.get(SslConfigs.SSL_PROTOCOL_CONFIG), is(SslConfigs.DEFAULT_SSL_PROTOCOL));

        LOGGER.info("Verifying that Kafka connect is stable");

        PodUtils.waitUntilPodsByNameStability(KafkaConnectResources.deploymentName(CLUSTER_NAME));
    }
}
