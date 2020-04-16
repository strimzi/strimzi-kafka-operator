/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.systemtest.resources.crd.KafkaClientsResource;
import io.strimzi.systemtest.resources.crd.KafkaConnectResource;
import io.strimzi.systemtest.resources.crd.KafkaResource;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

@Tag(REGRESSION)
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

        LOGGER.info("Verifying that Kafka Connect status is NotReady because of different TLS version");

        KafkaConnectUtils.waitForConnectStatus(CLUSTER_NAME, "NotReady");

        LOGGER.info("Replacing Kafka Connect config to the newest(TLSv1.2) one same as the Kafka broker has.");

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kafkaConnect -> kafkaConnect.getSpec().setConfig(configWithNewestVersionOfTls));

        LOGGER.info("Verifying that Kafka Connect has the excepted configuration:\n" +
            SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG + " -> {}\n" + SslConfigs.SSL_PROTOCOL_CONFIG + " -> {}",
            tlsVersion12, SslConfigs.DEFAULT_SSL_PROTOCOL);

        KafkaConnectUtils.waitForKafkaConnectConfigChange(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, tlsVersion12, NAMESPACE, CLUSTER_NAME);
        KafkaConnectUtils.waitForKafkaConnectConfigChange(SslConfigs.SSL_PROTOCOL_CONFIG, SslConfigs.DEFAULT_SSL_PROTOCOL, NAMESPACE, CLUSTER_NAME);

        LOGGER.info("Verifying that Kafka Connect is stable");

        PodUtils.waitUntilPodsByNameStability(KafkaConnectResources.deploymentName(CLUSTER_NAME));
    }

    @Test
    void testKafkaAndKafkaConnectCipherSuites() {
        Map<String, Object> configWithCipherSuitesSha384 = new HashMap<>();

        final String cipherSuitesSha384 = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
        final String cipherSuitesSha256 = "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256";

        configWithCipherSuitesSha384.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha384);

        LOGGER.info("Deploying Kafka cluster with the support {} cipher algorithms",  cipherSuitesSha384);

        KafkaResource.kafkaEphemeral(CLUSTER_NAME, 3)
            .editSpec()
                .editKafka()
                    .withConfig(configWithCipherSuitesSha384)
                .endKafka()
            .endSpec()
            .done();

        Map<String, Object> configsFromKafkaCustomResource = KafkaResource.kafkaClient().inNamespace(NAMESPACE).withName(CLUSTER_NAME).get().getSpec().getKafka().getConfig();

        LOGGER.info("Verifying that Kafka cluster has the excepted configuration:\n" + SslConfigs.SSL_CIPHER_SUITES_CONFIG + " -> {}",
            configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

        assertThat(configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG), is(cipherSuitesSha384));

        Map<String, Object> configWithCipherSuitesSha256 = new HashMap<>();

        configWithCipherSuitesSha256.put(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha256);

        KafkaClientsResource.deployKafkaClients(KAFKA_CLIENTS_NAME).done();

        KafkaConnectResource.kafkaConnectWithoutWait(KafkaConnectResource.defaultKafkaConnect(CLUSTER_NAME, CLUSTER_NAME, 1)
            .editSpec()
                .withConfig(configWithCipherSuitesSha256)
            .endSpec()
            .build());

        LOGGER.info("Verifying that Kafka Connect status is NotReady because of different cipher suites complexity of algorithm");

        KafkaConnectUtils.waitForConnectStatus(CLUSTER_NAME, "NotReady");

        LOGGER.info("Replacing Kafka Connect config to the cipher suites same as the Kafka broker has.");

        KafkaConnectResource.replaceKafkaConnectResource(CLUSTER_NAME, kafkaConnect -> kafkaConnect.getSpec().setConfig(configWithCipherSuitesSha384));

        LOGGER.info("Verifying that Kafka Connect has the excepted configuration:\n" +
            SslConfigs.SSL_CIPHER_SUITES_CONFIG + " -> {}", configsFromKafkaCustomResource.get(SslConfigs.SSL_CIPHER_SUITES_CONFIG));

        KafkaConnectUtils.waitForKafkaConnectConfigChange(SslConfigs.SSL_CIPHER_SUITES_CONFIG, cipherSuitesSha384, NAMESPACE, CLUSTER_NAME);

        LOGGER.info("Verifying that Kafka Connect is stable");

        PodUtils.waitUntilPodsByNameStability(KafkaConnectResources.deploymentName(CLUSTER_NAME));
    }
}
